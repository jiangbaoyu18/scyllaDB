/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2017 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "index/secondary_index_manager.hh"

#include "cql3/statements/index_target.hh"
#include "cql3/util.hh"
#include "index/target_parser.hh"
#include "db/query_context.hh"
#include "schema_builder.hh"
#include "db/view/view.hh"
#include "database.hh"

#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include "cassandra_types.h"
#include "types.hh"
#include "thrift/server.hh"


namespace secondary_index {

index::index(const sstring& target_column, const index_metadata& im)
    : _target_column{target_column}
    , _im{im}
{}

bool index::depends_on(const column_definition& cdef) const {
    return cdef.name_as_text() == _target_column;
}

bool index::supports_expression(const column_definition& cdef, const cql3::expr::oper_t op) const {
    return cdef.name_as_text() == _target_column && op == cql3::expr::oper_t::EQ;
}

const index_metadata& index::metadata() const {
    return _im;
}

secondary_index_manager::secondary_index_manager(column_family& cf)
    : _cf{cf}
{}

void secondary_index_manager::reload() {
    const auto& table_indices = _cf.schema()->all_indices();
    auto it = _indices.begin();
    while (it != _indices.end()) {
        auto index_name = it->first;
        if (!table_indices.contains(index_name)) {
            it = _indices.erase(it);
        } else {
            ++it;
        }
    }
    for (const auto& index : _cf.schema()->all_indices()) {
        add_index(index.second);
    }

    //init our mpp index info
    sstring index_info=_cf.schema()->comment();
    rjson::document  doc;
    if(!doc.Parse(index_info.c_str()).HasParseError()){
        if(doc.HasMember("use_mpp_index")&&doc["use_mpp_index"].IsBool()&&doc["use_mpp_index"].GetBool()==true){
            index_options_map  indexed_fields;
//            // in scyllaDB we only need to know which fields to be indexed, and the whole index info we send to SE
            if(doc.HasMember("fieldsOptions")&&doc["fieldsOptions"].IsArray()){
                rjson::value& fields_info = doc["fieldsOptions"];
                size_t len=fields_info.Size();
                for(size_t i = 0; i < len; i++){
                   bool indexed=fields_info[i]["indexed"].GetBool();
                    std::string cass_name=fields_info[i]["cass_name"].GetString();
                   if(indexed){
                       indexed_fields.emplace(std::move(cass_name),"true");
                   }
                }
            }
            index_metadata meta_data("mpp_index",indexed_fields,index_metadata_kind::custom,index_metadata::is_local_index(false));
            index mpp_index("target_column_is _in_index_metadata.index_options_map",meta_data);

            auto it = _mpp_indices.begin();
            while (it != _mpp_indices.end()) {
                auto index_name = it->first;
                if (index_name=="mpp_index") {
                    it = _mpp_indices.erase(it);
                } else {
                    ++it;
                }
            }
            _mpp_indices.emplace("mpp_index", std::move(mpp_index));

            if(thrift::get_thrift_client().local_is_initialized()&&this_shard_id()==0){ // we only need send mpp index info to SE ONCE (using shard 0)
                thrift::thrift_client& client=thrift::get_local_thrift_client();
                client.send_index_info_to_SE(index_info);
            }

        }
    }
}

void secondary_index_manager::on_finished(const frozen_mutation& m, partition_entry& pe){

    std::vector<secondary_index::index> indexes=list_mpp_indexes();
    if(indexes.size()>0){// exists mpp index
        const schema_ptr& m_schema=_cf.schema();
        cassandra::SelectRow parsed_row;
        std::unordered_set<sstring> column_names_set;

        //1. parsing partition key
        cassandra::SelectColumn parsed_column;
        partition_key key=m.key();
        uint8_t  idx=0;
        auto type_iterator = key.get_compound_type(*m_schema)->types().begin();
        schema::const_iterator_range_type pk_columns=m_schema->partition_key_columns();
        for (auto&& e : key.components(*m_schema)) {
            const column_definition& cd=pk_columns[idx];
            const sstring& column_name=cd.name_as_text();

            parsed_column.name=column_name;
            parsed_column.value=(*type_iterator)->to_string(to_bytes(e));
            parsed_column.column_type=0; // partition key
            parse_type_to_string((*type_iterator),parsed_column.type);
            parsed_row.columns.push_back(parsed_column);
            column_names_set.insert(column_name);

            ++type_iterator;
            ++idx;
        }

        //2.  parsing regular columns

        if(m_schema->clustering_key_size()==0){ // only one row in this partition

            partition_version& latest_version=*pe.version();
            mutation_partition& mp=latest_version.partition();
            // todo considering tombstone  of delete case
            for (const auto& re : mp.clustered_rows()) { // re is type of  `row_entry`
                const auto& row = re.row();

                row.cells().for_each_cell([&] (column_id& c_id, const atomic_cell_or_collection& cell) {
                    cassandra::SelectColumn parsed_column;

                    auto& column_def = (*m_schema).column_at(column_kind::regular_column, c_id);
                    const sstring& column_name=column_def.name_as_text();
                    // todo parse collection type here
                    const data_type& t=column_def.type;
                    const atomic_cell_view& acv=cell.as_atomic_cell(column_def);
                    if (acv.is_live()) {
                        const sstring& column_value=t->to_string( acv.value().linearize());
                        //todo acv also contains ttl timestamp info ,if we need

                        parsed_column.name=column_name;
                        parsed_column.value=column_value;
                        parsed_column.column_type=2; // regular column
                        parse_type_to_string(t,parsed_column.type);
                        parsed_row.columns.push_back(parsed_column);
                        column_names_set.insert(column_name);
                    }
                });
            }
        }else{
            // 3.todo parsing clustering key
        }

        for(auto& index:list_mpp_indexes()){
            const index_options_map& mpp_index_fields_info=index.metadata().options();
            bool is_first_write=true;

           for(auto& entry:  mpp_index_fields_info){
               if(!column_names_set.contains(entry.first)){
                   is_first_write=false;
                   break;
               }
           }

            /**
              * send indexed field data to SE after apply in memtable
              * 1. if all indexed fields are in memtable, we parse those fields data from memtable's `mutation_partition` and send to SE, without query disk sstable files  (which implies that this row is first write ,and when first write,we should contain all indexed fields (using default value if missing))
              * 2. if there are some indexed field not presented in memtable ,which implies that this write is a update, and we should read data from sstable files. and finally send all indexed fields data to SE
              */

           if(is_first_write){
               if(thrift::get_thrift_client().local_is_initialized()){
                   thrift::thrift_client& client=thrift::get_local_thrift_client();
                   client.send_indexed_fields_to_SE(parsed_row);
               }else{
                   std::cout<<"replaying commit log: {} and thrift client has not bean initialized"<<std::endl;
                   //todo when replay commit log ,the thrift client has not bean initialized ,consider using a cache to save those data temporarily
               }
           }else{
               //todo
//                db = service::get_local_storage_proxy().get_db().local();

           }
        }

    }

}

void secondary_index_manager::add_index(const index_metadata& im) {
    sstring index_target = im.options().at(cql3::statements::index_target::target_option_name);
    sstring index_target_name = target_parser::get_target_column_name_from_string(index_target);
    _indices.emplace(im.name(), index{index_target_name, im});
}

sstring index_table_name(const sstring& index_name) {
    return format("{}_index", index_name);
}

sstring index_name_from_table_name(const sstring& table_name) {
    if (table_name.size() < 7 || !boost::algorithm::ends_with(table_name, "_index")) {
        throw std::runtime_error(format("Table {} does not have _index suffix", table_name));
    }
    return table_name.substr(0, table_name.size() - 6); // remove the _index suffix from an index name;
}

static bytes get_available_token_column_name(const schema& schema) {
    bytes base_name = "idx_token";
    bytes accepted_name = base_name;
    int i = 0;
    while (schema.get_column_definition(accepted_name)) {
        accepted_name = base_name + to_bytes("_")+ to_bytes(std::to_string(++i));
    }
    return accepted_name;
}

view_ptr secondary_index_manager::create_view_for_index(const index_metadata& im) const {
    auto schema = _cf.schema();
    sstring index_target_name = im.options().at(cql3::statements::index_target::target_option_name);
    schema_builder builder{schema->ks_name(), index_table_name(im.name())};
    auto target_info = target_parser::parse(schema, im);
    const auto* index_target = im.local() ? target_info.ck_columns.front() : target_info.pk_columns.front();
    auto target_type = target_info.type;
    if (target_type != cql3::statements::index_target::target_type::values) {
        throw std::runtime_error(format("Unsupported index target type: {}", to_sstring(target_type)));
    }

    // For local indexing, start with base partition key
    if (im.local()) {
        if (index_target->is_partition_key()) {
            throw exceptions::invalid_request_exception("Local indexing based on partition key column is not allowed,"
                    " since whole base partition key must be used in queries anyway. Use global indexing instead.");
        }
        for (auto& col : schema->partition_key_columns()) {
            builder.with_column(col.name(), col.type, column_kind::partition_key);
        }
        builder.with_column(index_target->name(), index_target->type, column_kind::clustering_key);
    } else {
        builder.with_column(index_target->name(), index_target->type, column_kind::partition_key);
        // Additional token column is added to ensure token order on secondary index queries
        bytes token_column_name = get_available_token_column_name(*schema);
        builder.with_computed_column(token_column_name, bytes_type, column_kind::clustering_key, std::make_unique<token_column_computation>());
        for (auto& col : schema->partition_key_columns()) {
            if (col == *index_target) {
                continue;
            }
            builder.with_column(col.name(), col.type, column_kind::clustering_key);
        }
    }

    for (auto& col : schema->clustering_key_columns()) {
        if (col == *index_target) {
            continue;
        }
        builder.with_column(col.name(), col.type, column_kind::clustering_key);
    }
    if (index_target->is_primary_key()) {
        for (auto& def : schema->regular_columns()) {
            db::view::create_virtual_column(builder, def.name(), def.type);
        }
    }
    const sstring where_clause = format("{} IS NOT NULL", index_target->name_as_cql_string());
    builder.with_view_info(*schema, false, where_clause);
    return view_ptr{builder.build()};
}

std::vector<index_metadata> secondary_index_manager::get_dependent_indices(const column_definition& cdef) const {
    return boost::copy_range<std::vector<index_metadata>>(_indices
           | boost::adaptors::map_values
           | boost::adaptors::filtered([&] (auto& index) { return index.depends_on(cdef); })
           | boost::adaptors::transformed([&] (auto& index) { return index.metadata(); }));
}

std::vector<index> secondary_index_manager::list_indexes() const {
    return boost::copy_range<std::vector<index>>(_indices | boost::adaptors::map_values);
}
std::vector<index> secondary_index_manager::list_mpp_indexes() const {
    return boost::copy_range<std::vector<index>>(_mpp_indices | boost::adaptors::map_values);
}

bool secondary_index_manager::is_index(view_ptr view) const {
    return is_index(*view);
}

bool secondary_index_manager::is_index(const schema& s) const {
    return boost::algorithm::any_of(_indices | boost::adaptors::map_values, [&s] (const index& i) {
        return s.cf_name() == index_table_name(i.metadata().name());
    });
}

bool secondary_index_manager::is_global_index(const schema& s) const {
    return boost::algorithm::any_of(_indices | boost::adaptors::map_values, [&s] (const index& i) {
        return !i.metadata().local() && s.cf_name() == index_table_name(i.metadata().name());
    });
}

}
