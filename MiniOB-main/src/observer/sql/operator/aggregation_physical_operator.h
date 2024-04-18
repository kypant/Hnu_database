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
// Created by Sanyu on 2024/04/14.
//

#pragma once


#include "sql/expr/tuple.h"
#include "sql/operator/physical_operator.h"
#include "sql/parser/aggregation.h"
#include "sql/parser/value.h"

#include <vector>

class Trx;

class AggregationPhysicalOperator : public PhysicalOperator
{
public:
  AggregationPhysicalOperator(const std::vector<AggrType> aggrs) 
      : aggr_exprs_(aggrs)
  {}

  virtual ~AggregationPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::AGGR; }

  void add(AggrType aggrtype) { aggr_exprs_.push_back(aggrtype); }

  std::string name() const override { return "AGGR"; }
  std::string param() const override { return ""; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  int cell_num() const { return tuple_.cell_num(); }

  Tuple *current_tuple() override { return &tuple_; }


private:
  std::vector<AggrType>                   aggr_exprs_;
  ValueListTuple                           tuple_;
};
