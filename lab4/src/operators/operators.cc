
#include "operators/operators.h"

#include <cassert>
#include <functional>
#include <string>
#include <iostream>
#include <algorithm>
#include "common/macros.h"

#define UNUSED(p) ((void)(p))
using namespace std;
namespace buzzdb {
namespace operators {
Register::Register(uint64_t value)
      :num(value), type(Type::INT64) {}

Register::Register(string value)
      :str(value), type(Type::CHAR16) {}

Register Register::from_int(int64_t value) {
  return Register(value);
}

Register Register::from_string(const std::string& value) {
  return Register(value);
}

Register::Type Register::get_type() const {
  return this->type;
}

int64_t Register::as_int() const {
  return this->num;
}

std::string Register::as_string() const {
  return this->str;
}

uint64_t Register::get_hash() const {
  if(type == Type::INT64){
    return static_cast<uint64_t>(hash<int>{}(this->num));
  }
  else{
    return static_cast<uint64_t>(hash<string>{}(this->str));
  }
}

bool operator==(const Register& r1, const Register& r2) {
  if(r1.type != r2.type) return false;
  switch (r1.get_type()) {
    case Register::Type::INT64:
        return r1.num == r2.num;
    case Register::Type::CHAR16:
        return r1.str == r2.str;
  }
  return false;
}

bool operator!=(const Register& r1, const Register& r2) {
  if(r1.type != r2.type) return true;
  switch (r1.get_type()) {
    case Register::Type::INT64:
        return r1.num != r2.num;
    case Register::Type::CHAR16:
        return r1.str != r2.str;
  }
  return false;
}

bool operator<(const Register& r1, const Register& r2) {
  switch (r1.get_type()) {
    case Register::Type::INT64:
        return r1.num < r2.num;
    case Register::Type::CHAR16:
        return r1.str < r2.str;
  }
  return false;
}

bool operator<=(const Register& r1, const Register& r2) {
  switch (r1.get_type()) {
    case Register::Type::INT64:
        return r1.num <= r2.num;
    case Register::Type::CHAR16:
        return r1.str <= r2.str;
  }
  return false;
}

bool operator>(const Register& r1, const Register& r2) {
  switch (r1.get_type()) {
    case Register::Type::INT64:
        return r1.num > r2.num;
    case Register::Type::CHAR16:
        return r1.str > r2.str;
  }
  return false;
}

bool operator>=(const Register& r1, const Register& r2) {
  switch (r1.get_type()) {
    case Register::Type::INT64:
        return r1.num >= r2.num;
    case Register::Type::CHAR16:
        return r1.str >= r2.str;
  }
  return false;
}

Print::Print(Operator& input, std::ostream& stream) 
: UnaryOperator(input), stream(stream) {}

Print::~Print() = default;

void Print::open() {
  input->open();
}

bool Print::next() {
  // Get tuples from input. Output stream;
  if(input->next()){    //每次做next会将input->get_output变成新的row。每次call next，然后通过get_output获得最新的row。一直call next直到return false
    vector<Register*> input_tuple = input->get_output();    //处理当前的
    size_t reg_itr = 0 ;
    size_t tuple_size = input_tuple.size(); //避免空的input_tuple造成"\n";
    if(!tuple_size) return true; 
    for(auto reg : input_tuple){
      if(reg->get_type() == Register::Type::INT64){
        stream << reg->as_int();
      } 
      else if(reg->get_type() == Register::Type:: CHAR16 ) { 
        stream << reg->as_string();
      }
      if(reg_itr++ != tuple_size - 1 ) { 
        stream << ',' ;
      }
    }
  stream.put('\n'); // get new line at the end of row.
  return true;
  }
  return false;
}

void Print::close() {
  input->close();
  stream.clear();
}

std::vector<Register*> Print::get_output() {
  // Print has no output
  return {};
}

Projection::Projection(Operator& input, std::vector<size_t> attr_indexes)
    : UnaryOperator(input), attr_indexes(attr_indexes){}

Projection::~Projection() = default;

void Projection::open() {
  input->open();
}

bool Projection::next() {
  if(input->next()){
    vector<Register*> input_tuple = input->get_output();
    vector<Register*> temp_tuple = {};
    for(auto index : attr_indexes){
      temp_tuple.push_back(input_tuple[index]);
    }
    output_tuple = move(temp_tuple); //use move segment 但因为是指针，效率没很大区别
    return true;
  }
  return false;
}

void Projection::close() {
  input->close();
}

std::vector<Register*> Projection::get_output() {
  return output_tuple;
}

Select::Select(Operator& input, PredicateAttributeInt64 predicate)
    : UnaryOperator(input), type(Select_Type::INT), num_predicate(predicate) {}

Select::Select(Operator& input, PredicateAttributeChar16 predicate)
    : UnaryOperator(input), type(Select_Type::STR), str_predicate(predicate) {}

Select::Select(Operator& input, PredicateAttributeAttribute predicate)
    : UnaryOperator(input), type(Select_Type::INNER), inner_predicate(predicate) {}

Select::~Select() = default;

void Select::open() {
  input->open();
}

bool Select::next() {
  if(input->next()){
    vector<Register*> input_tuple = input->get_output();
    if(type == Select_Type::INT){
      switch(num_predicate.predicate_type){
        case PredicateType::EQ :
            if(input_tuple[num_predicate.attr_index]->as_int() == num_predicate.constant) 
            output_tuple = input_tuple;
            else output_tuple = {};
            break;
        case PredicateType::NE :
            if(input_tuple[num_predicate.attr_index]->as_int() != num_predicate.constant) 
            output_tuple = input_tuple;
            else output_tuple = {};
            break;
        case PredicateType::GE :
            if(input_tuple[num_predicate.attr_index]->as_int() >= num_predicate.constant) 
            output_tuple = input_tuple;
            else output_tuple = {};
            break;
        case PredicateType::GT :
            if(input_tuple[num_predicate.attr_index]->as_int() > num_predicate.constant) 
            output_tuple = input_tuple;
            else output_tuple = {};
            break;
        case PredicateType::LE :
            if(input_tuple[num_predicate.attr_index]->as_int() <= num_predicate.constant) 
            output_tuple = input_tuple;
            else output_tuple = {};
            break;
        case PredicateType::LT :
            if(input_tuple[num_predicate.attr_index]->as_int() < num_predicate.constant) 
            output_tuple = input_tuple;
            else output_tuple = {};
            break;
        default:
            output_tuple = {};
      }
    }
    else if(type == Select_Type::STR){
      // Only have Equal test
      switch(str_predicate.predicate_type){
        case PredicateType::EQ :
            if(input_tuple[str_predicate.attr_index]->as_string() == str_predicate.constant) 
            output_tuple = input_tuple;
            else output_tuple = {};
            break;
        default:
            output_tuple = {};
      }
    }
    else{ // inner Compare
      // Only implement GE situation
      switch(inner_predicate.predicate_type){
        case PredicateType::GE :
            if(input_tuple[inner_predicate.attr_left_index]->as_int() >= input_tuple[inner_predicate.attr_right_index]->as_int()) 
            output_tuple = input_tuple;
            else output_tuple = {};
            break;
        default:
            output_tuple = {};
      }
  }
  return true;
  }
  return false;
}

void Select::close() {
  input->close();
}

std::vector<Register*> Select::get_output() {
  return output_tuple;
}

Sort::Sort(Operator& input, std::vector<Criterion> criteria)
    : UnaryOperator(input), criteria(move(criteria)), next_index(0), sort_index(0), load(false)
{}


Sort::~Sort() = default;

void Sort::open() {
  input->open();
}

bool Sort::next() {
  //sort 得从child拿到所有tuples 然后排序 然后一个一个给parent
  if(!load){
    load = true;
    while(this->input->next()){
        vector<Register*> input_tuple = this->input->get_output();
        //必须将value保存下来，因为input_tuple里面的Register*指向input里面的output_regs。而这个output_regs内容随着next改变
        //所以不能用指针.
        vector<Register> reg_tuple;
        for (auto &reg_ptr : input_tuple) {  
          reg_tuple.push_back(*reg_ptr);
        }
        sort_tuples.push_back(reg_tuple);
    }
    // Sort sort_tuples
    sort(sort_tuples.begin(), sort_tuples.end(), [this, criteria = this->criteria](const auto& tuple1, const auto& tuple2) {
          return compare_tuple(tuple1, tuple2, criteria);
      });
  }
  if(next_index < sort_tuples.size()){
    next_index++;
    return true;
  }
  return false;
}

std::vector<Register*> Sort::get_output() {
  if(sort_index < sort_tuples.size()){
    vector<Register*> output_regs;
    for (auto& reg:sort_tuples[sort_index]){   //将vector中的registers的指针全部放入output，return output
      output_regs.push_back(&reg);
    }
    sort_index++;
    return output_regs;
  }
  return {};
}

void Sort::close() {
  input->close();
  sort_tuples.clear();
  sort_index = 0;
  next_index = 0;
  load = false;
}

HashJoin::HashJoin(Operator& input_left, Operator& input_right,
                   size_t attr_index_left, size_t attr_index_right)
    : BinaryOperator(input_left, input_right), left_index(attr_index_left), right_index(attr_index_right), join(false), next_index(0), join_index(0)
{}

HashJoin::~HashJoin() = default;

void HashJoin::open() {
  input_left->open();
  input_right->open();
}

void print_tuple(vector<Register> temp_tuple){
  for(uint i=0;i<temp_tuple.size();i++){
    if(temp_tuple[i].get_type() == Register::Type::INT64){
      cout << temp_tuple[i].as_int()<<", ";
    }
    else{
      cout << temp_tuple[i].as_string()<<", ";
    }
  }
  cout<<endl;
  return;
}

bool HashJoin::next() {
  if(!join){
    // Save all tuples from two tables;
    while(input_left->next()){
      vector<Register> temp_tuple;
      vector<Register*> left_tuple = input_left->get_output();
      for(auto& reg_ptr: left_tuple){
        temp_tuple.push_back(*reg_ptr);
      }
      left_tuples.push_back(temp_tuple);
    }
    while(input_right->next()){
      vector<Register> temp_tuple;
      vector<Register*> right_tuple = input_right->get_output();
      for(auto& reg_ptr: right_tuple){
        temp_tuple.push_back(*reg_ptr);
      }
      right_tuples.push_back(temp_tuple);
    }
    // Match attribute
    for(auto& l_tuple : left_tuples){
      for(auto& r_tuple : right_tuples){
        if(l_tuple[left_index].get_hash() == r_tuple[right_index].get_hash()){
          vector<Register> join_tuple = l_tuple;
          join_tuple.insert(join_tuple.end(), r_tuple.begin(), r_tuple.end());
          join_tuples.push_back(join_tuple);
          }
      }
    }
    // Finish join
    join = true;
  }
  // Already load and join
  if(next_index < join_tuples.size()){
    next_index++;
    return true;
  }
  return false;
}

void HashJoin::close() {
  input_left->close();
  input_right->close();
  join_tuples.clear();
  join_index = 0;
  next_index = 0;
  join = false;
}

std::vector<Register*> HashJoin::get_output() {
  if(join_index < join_tuples.size()){
    vector<Register*> output_regs;
    for (auto& reg : join_tuples[join_index]) {   //将vector中的registers的指针全部放入output，return output
      output_regs.push_back(&reg);
    }
    join_index++;
    return output_regs;
  }
  return {};
}

HashAggregation::HashAggregation(Operator& input,
                                 std::vector<size_t> group_by_attrs,
                                 std::vector<AggrFunc> aggr_funcs)
    : UnaryOperator(input), aggr_funcs(move(aggr_funcs)), group_by_attrs(group_by_attrs),next_index(0), aggregate(false), min_max(false)
{}

HashAggregation::~HashAggregation() = default;

void HashAggregation::open() {
  input->open();
}

bool HashAggregation::next() {
  if(!aggregate){
  while(input->next()){
    vector<Register*> input_tuple = input->get_output();
    for(AggrFunc aggr_fun: aggr_funcs){
      switch (aggr_fun.func)
      {
      case AggrFunc::Func::MIN:
        if(!min_max){
          Min_reg = *input_tuple[aggr_fun.attr_index];
          Max_reg = *input_tuple[aggr_fun.attr_index];
          min_max = true;
          break;
        }
        if(*input_tuple[aggr_fun.attr_index] < Min_reg){
          Min_reg = *input_tuple[aggr_fun.attr_index];
        }
        break;
      case AggrFunc::Func::MAX:
        if(!min_max){
          Min_reg = *input_tuple[aggr_fun.attr_index];
          Max_reg = *input_tuple[aggr_fun.attr_index];
          min_max = true;
          break;
        }
        if(*input_tuple[aggr_fun.attr_index] > Max_reg){
          Max_reg = *input_tuple[aggr_fun.attr_index];
        }
        break;
      case AggrFunc::Func::SUM:
        for(auto attr : group_by_attrs){ // only one attr in test, for int;
          auto it = sum_hash.find(input_tuple[attr]->as_int());
          if(it == sum_hash.end()){
            sum_hash[input_tuple[attr]->as_int()] = input_tuple[aggr_fun.attr_index]->as_int();
          }
          else{
            sum_hash[input_tuple[attr]->as_int()] += input_tuple[aggr_fun.attr_index]->as_int();
          }
        }
        break;
      case AggrFunc::Func::COUNT:
        for(auto attr : group_by_attrs){ // only one attr in test, for int;
          auto it = sum_hash.find(input_tuple[attr]->as_int());
          if(it == sum_hash.end()){
            count_hash[input_tuple[attr]->as_int()] = 1;
          }
          else{
            count_hash[input_tuple[attr]->as_int()] ++;
          }
        }
        break;
      default:
        break;
      }
    }
  }
  if (sum_hash.size() > 0) {
      for(auto it:sum_hash){
        vector<Register> temp_tuple;
        temp_tuple.push_back(it.first);
        temp_tuple.push_back(it.second);
        temp_tuple.push_back(count_hash[it.first]);
        count_sum_tuples.push_back(temp_tuple);
      }
    }
  aggregate = true;
  }
  // finish load;
  if(min_max){
    output_tuple.push_back(&Min_reg);
    output_tuple.push_back(&Max_reg);
    min_max = false;
    return true;
  }
  else{
    if(next_index<count_sum_tuples.size()){
      vector<Register*> temp_output;
      for(auto& reg:count_sum_tuples[next_index]){
        temp_output.push_back(&reg);
      }
      output_tuple = temp_output;
      next_index++;
      return true;
    }
  }
  return false;
};

void HashAggregation::close() {
  input->close();
  aggregate = false;
  min_max = false;
  next_index = 0;
}

std::vector<Register*> HashAggregation::get_output() {
  return output_tuple;
}

Union::Union(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right), load(false), next_index(0), union_index(0)
{}

Union::~Union() = default;

void Union::open() {
  // TODO: add your implementation here
  input_left->open();
  input_right->open();
}

bool Union::next() {
  if(!load){
    vector<Register*> left_tuple, right_tuple;
    while(input_left->next()){
      left_tuple = input_left->get_output();
      hash_table.insert(left_tuple[0]->as_int());
    }
    while(input_right->next()){
      right_tuple = input_right->get_output();
      hash_table.insert(right_tuple[0]->as_int());
    }
    for(auto num: hash_table){
      Union_output.push_back(Register::from_int(num));
    }
    load = true;
  }
  if(next_index < Union_output.size()){
    next_index++;
    return true;
  }
  return false;
}

std::vector<Register*> Union::get_output() {
  vector<Register*> output_tuple;
  if(union_index<Union_output.size()){
    output_tuple.push_back(&Union_output[union_index]);
    union_index++;
  }
  return output_tuple;
}

void Union::close() {
  // TODO: add your implementation here
  Union_output.clear();
  input_left->close();
  input_right->close();
  load = false;
  union_index = 0;
}

UnionAll::UnionAll(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right), load(false), next_index(0), union_all_index(0)
{}

UnionAll::~UnionAll() = default;

void UnionAll::open() {
  input_left->open();
  input_right->open();
}

bool UnionAll::next() {
  if(!load){
      vector<Register*> left_tuple, right_tuple;
    while(input_left->next()){
      left_tuple = input_left->get_output();
      Union_all_output.push_back(*left_tuple[0]);
    }
    while(input_right->next()){
      right_tuple = input_right->get_output();
      Union_all_output.push_back(*right_tuple[0]);
    }
    load = true;
  }
  if(next_index < Union_all_output.size()){
    next_index++;
    return true;
  }
  return false;
}

std::vector<Register*> UnionAll::get_output() {
  vector<Register*> output_tuple;
  if(union_all_index<Union_all_output.size()){
    output_tuple.push_back(&Union_all_output[union_all_index]);
    union_all_index++;
  }
  return output_tuple;
}

void UnionAll::close() {
  Union_all_output.clear();
  input_left->close();
  input_right->close();
  load = false;
  union_all_index = 0;
}

Intersect::Intersect(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right), load(false), next_index(0), intersect_index(0)
{}

Intersect::~Intersect() = default;

void Intersect::open() {
  input_left->open();
  input_right->open();
}

bool Intersect::next() {
  if(!load){
    vector<Register*> left_tuple, right_tuple;
    while(input_left->next()){
      left_tuple = input_left->get_output();
      hash_table.insert(left_tuple[0]->get_hash());
    }
    while(input_right->next()){
      right_tuple = input_right->get_output();
      auto iter = hash_table.find(right_tuple[0]->get_hash());
      if(iter != hash_table.end()) {
        hash_table.erase(right_tuple[0]->get_hash());
        Intersect_output.push_back(*right_tuple[0]);
      }
    }
    load = true;
  }
  if(next_index < Intersect_output.size()){
    next_index++;
    return true;
  }
  return false;
}

std::vector<Register*> Intersect::get_output() {
  vector<Register*> output_tuple;
  if(intersect_index<Intersect_output.size()){
    output_tuple.push_back(&Intersect_output[intersect_index]);
    intersect_index++;
  }
  return output_tuple;
}

void Intersect::close() {
  Intersect_output.clear();
  input_left->close();
  input_right->close();
  load = false;
  intersect_index = 0;
}

IntersectAll::IntersectAll(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right), load(false), next_index(0), intersect_all_index(0)
{}

IntersectAll::~IntersectAll() = default;

void IntersectAll::open() {
  input_left->open();
  input_right->open();
}

bool IntersectAll::next() {
  if(!load){
    vector<Register*> left_tuple, right_tuple;
    while(input_left->next()){
      left_tuple = input_left->get_output();
      hash_table.insert(left_tuple[0]->get_hash());
    }
    while(input_right->next()){
      right_tuple = input_right->get_output();
      auto iter = hash_table.find(right_tuple[0]->get_hash());
      if (iter != hash_table.end()) {
        Intersect_all_output.push_back(*right_tuple[0]);
      }
    }
    load = true;
  }
  if(next_index < Intersect_all_output.size()){
    next_index++;
    return true;
  }
  return false;
}

std::vector<Register*> IntersectAll::get_output() {
  vector<Register*> output_tuple;
  if(intersect_all_index<Intersect_all_output.size()){
    output_tuple.push_back(&Intersect_all_output[intersect_all_index]);
    intersect_all_index++;
    return output_tuple;
  }
  return {};
}

void IntersectAll::close() {
  Intersect_all_output.clear();
  input_left->close();
  input_right->close();
  load = false;
  intersect_all_index = 0;
}

Except::Except(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right), load(false), next_index(0), except_index(0)
{}

Except::~Except() = default;

void Except::open() {
  input_left->open();
  input_right->open();
}

bool Except::next() {
    if(!load){
      vector<Register*> left_tuple, right_tuple;
    while(input_left->next()){
      left_tuple = input_left->get_output();
      hash_table.insert(left_tuple[0]->get_hash());
    }
    while(input_right->next()){
      right_tuple = input_right->get_output();
      auto iter = hash_table.find(right_tuple[0]->get_hash());
      if (iter != hash_table.end()) {
        hash_table.erase(right_tuple[0]->get_hash());
      }
    }
    for(auto num: hash_table){
      Except_output.push_back(Register::from_int(num));
    }
    load = true;
  }
  if(next_index < Except_output.size()){
    next_index++;
    return true;
  }
  return false;
}

std::vector<Register*> Except::get_output() {
  vector<Register*> output_tuple;
  if(except_index<Except_output.size()){
    output_tuple.push_back(&Except_output[except_index]);
    except_index++;
  }
  return output_tuple;
}

void Except::close() {
  Except_output.clear();
  input_left->close();
  input_right->close();
  load = false;
  except_index = 0;
  return;
}

ExceptAll::ExceptAll(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right), load(false), next_index(0), except_all_index(0)
{}

ExceptAll::~ExceptAll() = default;

void ExceptAll::open() {
  input_left->open();
  input_right->open();
}

bool ExceptAll::next() {
  if(!load){
      vector<Register*> left_tuple, right_tuple;
    while(input_left->next()){
      left_tuple = input_left->get_output();
      Except_all_output.push_back(*left_tuple[0]);
    }
    while(input_right->next()){
      right_tuple = input_right->get_output();
      // delete the element of right tuples;
      auto iter = find(Except_all_output.begin(),Except_all_output.end(),(*right_tuple[0]));
      if (iter != Except_all_output.end()) {
        Except_all_output.erase(iter);
      }
    }
    load = true;
  }
  if(next_index < Except_all_output.size()){
    next_index++;
    return true;
  }
  return false;
}

std::vector<Register*> ExceptAll::get_output() {
  vector<Register*> output_tuple;
  if(except_all_index < Except_all_output.size()){
    output_tuple.push_back(&Except_all_output[except_all_index]);
    except_all_index++;
  }
  return output_tuple;
}

void ExceptAll::close() {
  Except_all_output.clear();
  input_left->close();
  input_right->close();
  load = false;
  except_all_index = 0;
  return;
}

}  // namespace operators
}  // namespace buzzdb
