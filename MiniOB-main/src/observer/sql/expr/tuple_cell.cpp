/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/07/05.
//

#include "sql/expr/tuple_cell.h"
#include "common/lang/string.h"
#include "sql/parser/aggregation.h"

void set_aggralias(std::string alias, AggrType aggr_type,std::string &str){
  switch(aggr_type){
      case AggrType::SUM: {
        str = "SUM(" + alias + ")";
      } break;

      case AggrType::AVG: {
        str = "AVG(" + alias + ")";
      } break;

      case AggrType::MAX: {
        str = "MAX(" + alias + ")";
      } break;

      case AggrType::MIN: {
        str = "MIN(" + alias + ")";
      } break;

      case AggrType::COUNT: {
        str = "COUNT(*)";
      } break;

      default: {
        str = "UNKNOWN(" + alias + ")";
      }
    }
}

TupleCellSpec::TupleCellSpec(const char *table_name, const char *field_name, const char *alias)
{
  if (table_name) {
    table_name_ = table_name;
  }
  if (field_name) {
    field_name_ = field_name;
  }
  if (alias) {
    alias_ = alias;
  } else {
    if (table_name_.empty()) {
      alias_ = field_name_;
    } else {
      alias_ = table_name_ + "." + field_name_;
    }
  }
}

TupleCellSpec::TupleCellSpec(const char *table_name, const char *field_name,AggrType aggr_type,const char *alias)
{
  if (table_name) {
    table_name_ = table_name;
  }
  if (field_name) {
    field_name_ = field_name;
  }
  if(aggr_type == AggrType::DEFAULT){
    if (alias) {
    alias_ = alias;
    } else {
      if (table_name_.empty()) {
        alias_ = field_name_;
      } else {
        alias_ = table_name_ + "." + field_name_;
      }
    }
  } else {
    std::string str;
    if (table_name_.empty()) {
        set_aggralias(field_name_,aggr_type,str);
        alias = str.c_str();
      } else {
        set_aggralias(table_name_ + "." + field_name_,aggr_type,str);
        alias = str.c_str();
      }
  }
}

TupleCellSpec::TupleCellSpec(const char *alias)
{
  if (alias) {
    alias_ = alias;
  }
}

TupleCellSpec::TupleCellSpec(const char *alias,AggrType aggr_type)
{
  if (alias) {
    std::string str,alias_s(alias);
    set_aggralias(alias_s,aggr_type,str);
    alias_ = str.c_str();
  }
}