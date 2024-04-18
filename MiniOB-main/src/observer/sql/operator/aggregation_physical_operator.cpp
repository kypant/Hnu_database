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
// Created by Sanyu on 2024/4/14.
//

#include "sql/operator/aggregation_physical_operator.h"

#include <math.h>
#include <vector>

RC AggregationPhysicalOperator::open(Trx *trx) {
    if (children_.empty()) { return RC::SUCCESS; }

    std::unique_ptr<PhysicalOperator>& child = children_[0];
    RC                                 rc    = child->open(trx);
    if (rc != RC::SUCCESS)
    {
        LOG_WARN("failed to open child operator: %s", strrc(rc));
        return rc;
    }

    return RC::SUCCESS;
}

RC calculateAggrResult(std::vector<Value>& aggr_vals,AggrType aggr_type,Value &value){
    double sum = 0;
    int count = aggr_vals.size();
    AttrType attrtype = aggr_vals[0].attr_type();
    switch(aggr_type){

        case AggrType::SUM :{
          //calculate sum
          for(auto val : aggr_vals) {
            sum += (attrtype == AttrType::INTS) ? val.get_int() : val.get_float();
          }

          //set value
          value.set_type(attrtype);
          (attrtype == AttrType::INTS) ? value.set_int((int)sum) : value.set_float((float)sum);

        } break;

        case AggrType::AVG :{
          //calculate sum
          for(auto val : aggr_vals) {
            sum += (attrtype == AttrType::INTS) ? val.get_int() : val.get_float();
          }

          //get avg
          sum /= count;

          //set value
          value.set_type(AttrType::FLOATS);
          value.set_float((float)sum);

        } break;

        case AggrType::MAX :{
          Value MaxValue = aggr_vals[0];
          //calculate max
          for(auto val : aggr_vals) {
            if (MaxValue.compare(val) < 0) MaxValue = val;
          }

          //set value
          value.set_type(aggr_vals[0].attr_type());
          const Value result(MaxValue);
          value.set_value(result);
        } break;

        case AggrType::MIN :{
          Value MinValue = aggr_vals[0];
          //calculate min
          for(auto val : aggr_vals) {
            if (MinValue.compare(val) > 0) MinValue = val;
          }

          //set value
          value.set_type(aggr_vals[0].attr_type());
          const Value result(MinValue);
          value.set_value(result);

        } break;

        case AggrType::COUNT :{
          //set value
          value.set_type(AttrType::INTS);
          value.set_int(count);
        } break;

        default : {
            LOG_WARN("unsupported aggregation type");
            return RC::INTERNAL;
        }
      }

    return RC::SUCCESS;
}

RC AggregationPhysicalOperator::next() {
  RC                rc   = RC::SUCCESS;
  if (children_.empty()) return RC::SUCCESS;
  PhysicalOperator* oper = children_[0].get();
  if (!oper) return RC::INTERNAL;
  if (tuple_.cell_num() > 0) return RC::RECORD_EOF;
  
    size_t aggr_size = aggr_exprs_.size();
    std::vector<std::vector<Value>> aggr_results(aggr_size);

  while (RC::SUCCESS == (rc = oper->next())) {
    Tuple *tuple = oper->current_tuple();
    if (nullptr == tuple) {
      rc = RC::INTERNAL;
      LOG_WARN("failed to get tuple from operator");
      break;
    }

    //add tuple's values to aggr_results for aggregation functions
    for(size_t i = 0;i < aggr_size;++i) {
        Value value;
        rc = tuple->cell_at(i,value);
        if( rc != RC::SUCCESS) return rc;
        aggr_results[i].push_back(value);
    }
  }

    std::vector<Value> aggr_result(aggr_size);
    //calculate aggregation result and set the result into aggr_result
    for(size_t i = 0;i < aggr_results.size();i++){
        RC rc2 = calculateAggrResult(aggr_results[i],aggr_exprs_[i],aggr_result[i]);
        if(rc2 != RC::SUCCESS){
            rc = rc2;
            break;
        }
    }

    if (rc == RC::RECORD_EOF || aggr_results.empty())
    {
        tuple_.set_cells(aggr_result);
        rc = RC::SUCCESS;
    }

  return rc;
}



RC AggregationPhysicalOperator::close() {
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}
