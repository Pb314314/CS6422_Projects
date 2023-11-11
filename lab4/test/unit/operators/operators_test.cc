#include <gtest/gtest.h>
#include <algorithm>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "operators/operators.h"

namespace {

using namespace std::literals::string_literals;

using buzzdb::operators::Except;
using buzzdb::operators::ExceptAll;
using buzzdb::operators::HashAggregation;
using buzzdb::operators::HashJoin;
using buzzdb::operators::Intersect;
using buzzdb::operators::IntersectAll;
using buzzdb::operators::Print;
using buzzdb::operators::Projection;
using buzzdb::operators::Register;
using buzzdb::operators::Select;
using buzzdb::operators::Sort;
using buzzdb::operators::Union;
using buzzdb::operators::UnionAll;

TEST(OperatorsTest, Register) {
  auto reg_i1 = Register::from_int(12345);
  auto reg_i2 = Register::from_int(67890);
  auto reg_i3 = Register::from_int(12345);
  auto reg_s1 = Register::from_string("this is a string"s);
  auto reg_s2 = Register::from_string("yet another stri"s);
  auto reg_s3 = Register::from_string("this is a string"s);

  ASSERT_EQ(Register::Type::INT64, reg_i1.get_type());
  ASSERT_EQ(Register::Type::INT64, reg_i2.get_type());
  ASSERT_EQ(Register::Type::INT64, reg_i3.get_type());
  ASSERT_EQ(Register::Type::CHAR16, reg_s1.get_type());
  ASSERT_EQ(Register::Type::CHAR16, reg_s2.get_type());
  ASSERT_EQ(Register::Type::CHAR16, reg_s3.get_type());

  EXPECT_EQ(12345, reg_i1.as_int());
  EXPECT_EQ(67890, reg_i2.as_int());
  EXPECT_EQ(12345, reg_i3.as_int());
  EXPECT_EQ("this is a string"s, reg_s1.as_string());
  EXPECT_EQ("yet another stri"s, reg_s2.as_string());
  EXPECT_EQ("this is a string"s, reg_s3.as_string());

  EXPECT_NE(reg_i1, reg_i2);
  EXPECT_EQ(reg_i1, reg_i3);
  EXPECT_NE(reg_i1, reg_s1);
  EXPECT_NE(reg_s1, reg_s2);
  EXPECT_EQ(reg_s1, reg_s3);
  EXPECT_NE(reg_s1, reg_i2);

  EXPECT_LT(reg_i1, reg_i2);
  EXPECT_LE(reg_i1, reg_i2);
  EXPECT_GT(reg_i2, reg_i3);
  EXPECT_GE(reg_i2, reg_i3);
  EXPECT_GE(reg_i1, reg_i3);

  EXPECT_LT(reg_s1, reg_s2);
  EXPECT_LE(reg_s1, reg_s2);
  EXPECT_GT(reg_s2, reg_s3);
  EXPECT_GE(reg_s2, reg_s3);
  EXPECT_GE(reg_s1, reg_s3);

  EXPECT_EQ(reg_i1.get_hash(), reg_i3.get_hash());
  EXPECT_EQ(reg_s1.get_hash(), reg_s3.get_hash());
}

// convert int into register
Register convert_to_register(int64_t value) {
  return Register::from_int(value);
}

// convert string into register
Register convert_to_register(const std::string& value) {
  return Register::from_string(value);
}

// 这个模版，Ts包含传递的类型的包。 Is包含传递参数的索引。 可以传递多个参数
// fold expression 
//将 tuple中的数值(比如： {24002, "Xenokrates      "}) 变成vector<Register>：{reg:24002, reg:"Xenokrates      "}
template <typename... Ts, size_t... Is>
void write_to_registers_impl(std::vector<Register>& registers,
                             const std::tuple<Ts...>& tuple,
                             std::index_sequence<Is...>) {
  ((registers[Is] = convert_to_register(std::get<Is>(tuple))), ...);
}
// 这两个函数 就是将一个任意大小的tuple,里面都是些int或者string，一个个变成register， 写进registers里面
template <typename... Ts>
void write_to_registers(std::vector<Register>& registers,
                        const std::tuple<Ts...>& tuple) {
  write_to_registers_impl(registers, tuple, std::index_sequence_for<Ts...>{});
}

//
template <typename... Ts>
//通过这个类，将不同类型的row变成vecor<Register*>类型
class TestTupleSource : public buzzdb::operators::Operator {
 private:
  const std::vector<std::tuple<Ts...>>& tuples;   //一个可以vector 里面的tuple是可以有任意个类型的attributes的tuple（tuple可以包含不定数量和类型的元素）
  size_t current_index = 0;
  std::vector<Register> output_regs;

 public:
  bool opened = false;
  bool closed = false;

  explicit TestTupleSource(const std::vector<std::tuple<Ts...>>& tuples)  //这个输入就是一个table 每个tuple是表中的一行数据
      : tuples(tuples) {}

  void open() override {
    output_regs.resize(sizeof...(Ts));  // 注意，每个tuple的元素个数是一样的。
    opened = true;
  }

  bool next() override {    // 将下一个row放入当前的output_regs
    if (current_index < tuples.size()) {
      write_to_registers(output_regs, tuples[current_index]); //取一个tuple 变成vector<Register>。 每一个register，记录一个attribute的内容。
      ++current_index;
      return true;
    } else {
      return false;
    }
  }

  void close() override {
    output_regs.clear();
    closed = true;
  }

  std::vector<Register*> get_output() override {    //将当前的output_regs 变成vector<Register*>返回
    std::vector<Register*> output;
    output.reserve(sizeof...(Ts));// output_regs里面存着数据，在heap中
    for (auto& reg : output_regs) {   //将vector中的registers的指针全部放入output，return output
      output.push_back(&reg);
    }
    return output;
  }
};

const std::vector<std::tuple<int64_t, std::string>> relation_students{
    {24002, "Xenokrates      "},
    {26120, "Fichte          "},
    {29555, "Feuerbach       "},
};

const std::vector<std::tuple<int64_t, int64_t, int64_t>> relation_grades{
    {24002, 5001, 1},
    {24002, 5041, 2},
    {29555, 4630, 2},
};

const std::vector<std::tuple<int64_t>> relation_set_a{{1, 1, 2, 3, 3, 3}};
const std::vector<std::tuple<int64_t>> relation_set_b{{2, 4, 4, 3, 3}};


std::string sort_output(const std::string& str) {
  std::vector<std::string> lines;
  size_t str_pos = 0;
  while (true) {
    size_t new_str_pos = str.find('\n', str_pos);
    if (new_str_pos == std::string::npos) {
      lines.emplace_back(&str[str_pos], str.size() - str_pos);
      break;
    }
    lines.emplace_back(&str[str_pos], new_str_pos - str_pos + 1);
    str_pos = new_str_pos + 1;
    if (str_pos == str.size()) {
      break;
    }
  }
  std::sort(lines.begin(), lines.end());
  std::string sorted_str;
  for (auto& line : lines) {
    sorted_str.append(line);
  }
  return sorted_str;
}

TEST(OperatorsTest, Print) {
  TestTupleSource source{relation_students};
  std::stringstream output;
  Print print{source, output};

  print.open();
  EXPECT_TRUE(source.opened);
  EXPECT_FALSE(source.closed);
  while (print.next()) {
  }
  print.close();
  EXPECT_TRUE(source.closed);

  auto expected_output =
      ("24002,Xenokrates      \n"
       "26120,Fichte          \n"
       "29555,Feuerbach       \n"s);
  EXPECT_EQ(expected_output, output.str());
}

TEST(OperatorsTest, Projection) {
  TestTupleSource source{relation_students};
  Projection projection{source, {0}};
  std::stringstream output;
  Print print{projection, output};

  print.open();
  EXPECT_TRUE(source.opened);
  EXPECT_FALSE(source.closed);
  while (print.next()) {
  }
  print.close();
  EXPECT_TRUE(source.closed);

  auto expected_output =
      ("24002\n"
       "26120\n"
       "29555\n"s);
  EXPECT_EQ(expected_output, sort_output(output.str()));
}

TEST(OperatorsTest, SelectIntEq) {
  TestTupleSource source{relation_students};
  Select select{source, Select::PredicateAttributeInt64{
                            0, 26120, Select::PredicateType::EQ}};
  std::stringstream output;
  Print print{select, output};

  print.open();
  EXPECT_TRUE(source.opened);
  EXPECT_FALSE(source.closed);
  while (print.next()) {
  }
  print.close();
  EXPECT_TRUE(source.closed);

  auto expected_output = ("26120,Fichte          \n"s);
  EXPECT_EQ(expected_output, sort_output(output.str()));
}

TEST(OperatorsTest, SelectStringEq) {
  TestTupleSource source{relation_students};
  Select select{source, Select::PredicateAttributeChar16{
                            1, "Feuerbach       ", Select::PredicateType::EQ}};
  std::stringstream output;
  Print print{select, output};

  print.open();
  EXPECT_TRUE(source.opened);
  EXPECT_FALSE(source.closed);
  while (print.next()) {
  }
  print.close();
  EXPECT_TRUE(source.closed);

  auto expected_output = ("29555,Feuerbach       \n"s);
  EXPECT_EQ(expected_output, sort_output(output.str()));
}

TEST(OperatorsTest, SelectIntNe) {
  TestTupleSource source{relation_students};
  Select select{source, Select::PredicateAttributeInt64{
                            0, 26120, Select::PredicateType::NE}};
  std::stringstream output;
  Print print{select, output};

  print.open();
  EXPECT_TRUE(source.opened);
  EXPECT_FALSE(source.closed);
  while (print.next()) {
  }
  print.close();
  EXPECT_TRUE(source.closed);

  auto expected_output =
      ("24002,Xenokrates      \n"s
       "29555,Feuerbach       \n"s);
  EXPECT_EQ(expected_output, sort_output(output.str()));
}

TEST(OperatorsTest, SelectIntLower) {
  for (auto ptype : {Select::PredicateType::LT, Select::PredicateType::LE}) {
    TestTupleSource source{relation_students};
    Select select{source, Select::PredicateAttributeInt64{0, 25000, ptype}};
    std::stringstream output;
    Print print{select, output};

    print.open();
    EXPECT_TRUE(source.opened);
    EXPECT_FALSE(source.closed);
    while (print.next()) {
    }
    print.close();
    EXPECT_TRUE(source.closed);

    auto expected_output = ("24002,Xenokrates      \n"s);
    EXPECT_EQ(expected_output, sort_output(output.str()));
  }
}

TEST(OperatorsTest, SelectIntGreater) {
  for (auto ptype : {Select::PredicateType::GT, Select::PredicateType::GE}) {
    TestTupleSource source{relation_students};
    Select select{source, Select::PredicateAttributeInt64{0, 25000, ptype}};
    std::stringstream output;
    Print print{select, output};

    print.open();
    EXPECT_TRUE(source.opened);
    EXPECT_FALSE(source.closed);
    while (print.next()) {
    }
    print.close();
    EXPECT_TRUE(source.closed);

    auto expected_output =
        ("26120,Fichte          \n"
         "29555,Feuerbach       \n"s);
    EXPECT_EQ(expected_output, sort_output(output.str()));
  }
}

TEST(OperatorsTest, SelectAttrAttr) {
  static const std::vector<std::tuple<int64_t, int64_t>> relation_numbers{
      {1, 1}, {1, 2}, {1, 3}, {1, 4}, {2, 1}, {2, 3}, {3, 2}};
  TestTupleSource source{relation_numbers};
  Select select{source, Select::PredicateAttributeAttribute{
                            0, 1, Select::PredicateType::GE}};
  std::stringstream output;
  Print print{select, output};

  print.open();
  EXPECT_TRUE(source.opened);
  EXPECT_FALSE(source.closed);
  while (print.next()) {
  }
  print.close();
  EXPECT_TRUE(source.closed);

  auto expected_output =
      ("1,1\n"
       "2,1\n"
       "3,2\n"s);
  EXPECT_EQ(expected_output, sort_output(output.str()));
}

TEST(OperatorsTest, Sort) {
  TestTupleSource source{relation_grades};
  Sort sort{source, {{0, true}, {2, false}}}; //通过多个key进行排序
  std::stringstream output;
  Print print{sort, output};

  print.open();
  EXPECT_TRUE(source.opened);
  EXPECT_FALSE(source.closed);
  while (print.next()) {
  }
  print.close();
  EXPECT_TRUE(source.closed);

  auto expected_output =
      ("29555,4630,2\n"
       "24002,5001,1\n"
       "24002,5041,2\n"s);
  EXPECT_EQ(expected_output, output.str());
}

TEST(OperatorsTest, HashJoin) {
  TestTupleSource source_students{relation_students};
  TestTupleSource source_grades{relation_grades};
  HashJoin join{source_students, source_grades, 0, 0};
  std::stringstream output;
  Print print{join, output};

  print.open();
  EXPECT_TRUE(source_students.opened);
  EXPECT_TRUE(source_grades.opened);
  EXPECT_FALSE(source_students.closed);
  EXPECT_FALSE(source_grades.closed);
  while (print.next()) {
  }
  print.close();
  EXPECT_TRUE(source_students.closed);
  EXPECT_TRUE(source_grades.closed);

  auto expected_output =
      ("24002,Xenokrates      ,24002,5001,1\n"
       "24002,Xenokrates      ,24002,5041,2\n"
       "29555,Feuerbach       ,29555,4630,2\n"s);
  EXPECT_EQ(expected_output, sort_output(output.str()));
}

TEST(OperatorsTest, HashAggregationMinMax) {
  TestTupleSource source{relation_students};
  HashAggregation aggregation{
      source,
      {},
      {
          HashAggregation::AggrFunc{HashAggregation::AggrFunc::MIN, 1},
          HashAggregation::AggrFunc{HashAggregation::AggrFunc::MAX, 1},
      }};
  std::stringstream output;
  Print print{aggregation, output};

  print.open();
  EXPECT_TRUE(source.opened);
  EXPECT_FALSE(source.closed);
  while (print.next()) {
  }
  print.close();
  EXPECT_TRUE(source.closed);

  auto expected_output = ("Feuerbach       ,Xenokrates      \n"s);
  EXPECT_EQ(expected_output, sort_output(output.str()));
}

TEST(OperatorsTest, HashAggregationSumCount) {
  TestTupleSource source{relation_grades};
  HashAggregation aggregation{
      source,
      {0},
      {
          HashAggregation::AggrFunc{HashAggregation::AggrFunc::SUM, 2},
          HashAggregation::AggrFunc{HashAggregation::AggrFunc::COUNT, 0},
      }};
  std::stringstream output;
  Print print{aggregation, output};

  print.open();
  EXPECT_TRUE(source.opened);
  EXPECT_FALSE(source.closed);
  while (print.next()) {
  }
  print.close();
  EXPECT_TRUE(source.closed);

  auto expected_output =
      ("24002,3,2\n"
       "29555,2,1\n"s);
  EXPECT_EQ(expected_output, sort_output(output.str()));
}

TEST(AdvancedOperatorsTest, Union) {
  TestTupleSource source_left{relation_set_a};
  TestTupleSource source_right{relation_set_b};
  Union union_{source_left, source_right};
  std::stringstream output;
  Print print{union_, output};

  print.open();
  EXPECT_TRUE(source_left.opened);
  EXPECT_TRUE(source_right.opened);
  EXPECT_FALSE(source_left.closed);
  EXPECT_FALSE(source_right.closed);
  while (print.next()) {
  }
  print.close();
  EXPECT_TRUE(source_left.closed);
  EXPECT_TRUE(source_right.closed);

  auto expected_output =
      ("1\n"
       "2\n"
       "3\n"
       "4\n"s);
  EXPECT_EQ(expected_output, sort_output(output.str()));
}

TEST(AdvancedOperatorsTest, UnionAll) {
  TestTupleSource source_left{relation_set_a};
  TestTupleSource source_right{relation_set_b};
  UnionAll union_{source_left, source_right};
  std::stringstream output;
  Print print{union_, output};

  print.open();
  EXPECT_TRUE(source_left.opened);
  EXPECT_TRUE(source_right.opened);
  EXPECT_FALSE(source_left.closed);
  EXPECT_FALSE(source_right.closed);
  while (print.next()) {
  }
  print.close();
  EXPECT_TRUE(source_left.closed);
  EXPECT_TRUE(source_right.closed);

  auto expected_output =
      ("1\n"
       "1\n"
       "2\n"
       "2\n"
       "3\n"
       "3\n"
       "3\n"
       "3\n"
       "3\n"
       "4\n"
       "4\n"s);
  EXPECT_EQ(expected_output, sort_output(output.str()));
}

TEST(BonusOperatorsTest, Intersect) {
  TestTupleSource source_left{relation_set_a};
  TestTupleSource source_right{relation_set_b};
  Intersect intersect{source_left, source_right};
  std::stringstream output;
  Print print{intersect, output};

  print.open();
  EXPECT_TRUE(source_left.opened);
  EXPECT_TRUE(source_right.opened);
  EXPECT_FALSE(source_left.closed);
  EXPECT_FALSE(source_right.closed);
  while (print.next()) {
  }
  print.close();
  EXPECT_TRUE(source_left.closed);
  EXPECT_TRUE(source_right.closed);

  auto expected_output =
      ("2\n"
       "3\n"s);
  EXPECT_EQ(expected_output, sort_output(output.str()));
}

TEST(BonusOperatorsTest, IntersectAll) {
  TestTupleSource source_left{relation_set_a};
  TestTupleSource source_right{relation_set_b};
  IntersectAll intersect{source_left, source_right};
  std::stringstream output;
  Print print{intersect, output};

  print.open();
  EXPECT_TRUE(source_left.opened);
  EXPECT_TRUE(source_right.opened);
  EXPECT_FALSE(source_left.closed);
  EXPECT_FALSE(source_right.closed);
  while (print.next()) {
  }
  print.close();
  EXPECT_TRUE(source_left.closed);
  EXPECT_TRUE(source_right.closed);

  auto expected_output =
      ("2\n"
       "3\n"
       "3\n"s);
  EXPECT_EQ(expected_output, sort_output(output.str()));
}

TEST(BonusOperatorsTest, Except) {
  TestTupleSource source_left{relation_set_a};
  TestTupleSource source_right{relation_set_b};
  Except except{source_left, source_right};
  std::stringstream output;
  Print print{except, output};

  print.open();
  EXPECT_TRUE(source_left.opened);
  EXPECT_TRUE(source_right.opened);
  EXPECT_FALSE(source_left.closed);
  EXPECT_FALSE(source_right.closed);
  while (print.next()) {
  }
  print.close();
  EXPECT_TRUE(source_left.closed);
  EXPECT_TRUE(source_right.closed);

  auto expected_output = ("1\n"s);
  EXPECT_EQ(expected_output, sort_output(output.str()));
}

TEST(BonusOperatorsTest, ExceptAll) {
  TestTupleSource source_left{relation_set_a};
  TestTupleSource source_right{relation_set_b};
  ExceptAll except{source_left, source_right};
  std::stringstream output;
  Print print{except, output};

  print.open();
  EXPECT_TRUE(source_left.opened);
  EXPECT_TRUE(source_right.opened);
  EXPECT_FALSE(source_left.closed);
  EXPECT_FALSE(source_right.closed);
  while (print.next()) {
  }
  print.close();
  EXPECT_TRUE(source_left.closed);
  EXPECT_TRUE(source_right.closed);

  auto expected_output =
      ("1\n"
       "1\n"
       "3\n"s);
  EXPECT_EQ(expected_output, sort_output(output.str()));
}

}  // namespace

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
