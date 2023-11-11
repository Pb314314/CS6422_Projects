#pragma once

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <vector>
#include <queue>
#include <unordered_set>
#include <unordered_map>
#include "common/macros.h"

using namespace std;
namespace buzzdb {
namespace operators {

class Register {
 public:
  enum class Type { INT64, CHAR16 };

  Register() = default;
  Register(const Register&) = default;
  Register(Register&&) = default;
  Register(uint64_t value);

  Register(string value);
  
  Register& operator=(const Register&) = default;   // 拷贝构造
  Register& operator=(Register&&) = default;        //移动构造

  /// Creates a `Register` from a given `int64_t`.
  static Register from_int(int64_t value);

  /// Creates a `Register` from a given `std::string`. The register must only
  /// be able to hold fixed size strings of size 16, so `value` must be at
  /// most 16 characters long.
  static Register from_string(const std::string& value);

  /// Returns the type of the register.
  Type get_type() const;

  /// Returns the `int64_t` value for this register. Must only be called when
  /// this register really is an integer.
  int64_t as_int() const;

  /// Returns the `std::string` value for this register. Must only be called
  /// when this register really is a string.
  std::string as_string() const;

  /// Returns the hash value for this register.
  uint64_t get_hash() const;

  /// Compares two register for equality.
  friend bool operator==(const Register& r1, const Register& r2);

  /// Compares two registers for inequality.
  friend bool operator!=(const Register& r1, const Register& r2);

  /// Compares two registers for `<`. Must only be called when `r1` and `r2`
  /// have the same type.
  friend bool operator<(const Register& r1, const Register& r2);

  /// Compares two registers for `<=`. Must only be called when `r1` and `r2`
  /// have the same type.
  friend bool operator<=(const Register& r1, const Register& r2);

  /// Compares two registers for `>`. Must only be called when `r1` and `r2`
  /// have the same type.
  friend bool operator>(const Register& r1, const Register& r2);

  /// Compares two registers for `>=`. Must only be called when `r1` and `r2`
  /// have the same type.
  friend bool operator>=(const Register& r1, const Register& r2);

 private:
  // TODO: add your implementation here
  
  string   str;
  uint64_t num;
  Type     type;
};

class Operator {
 public:
  virtual ~Operator() = default;

  /// Initializes the operator.
  virtual void open() = 0;

  /// Tries to generate the next tuple. Return true when a new tuple is
  /// available.
  virtual bool next() = 0;

  /// Destroys the operator.
  virtual void close() = 0;

  /// This returns the pointers to the registers of the generated tuple. When
  /// `next()` returns true, the Registers will contain the values for the
  /// next tuple. Each `Register*` in the vector stands for one attribute of
  /// the tuple.
  virtual std::vector<Register*> get_output() = 0;
};

class UnaryOperator : public Operator {
 protected:
  Operator* input;

 public:
  explicit UnaryOperator(Operator& input) : input(&input) {}

  ~UnaryOperator() override = default;
};

class BinaryOperator : public Operator {
 protected:
  Operator* input_left;
  Operator* input_right;

 public:
  explicit BinaryOperator(Operator& input_left, Operator& input_right)
      : input_left(&input_left), input_right(&input_right) {}

  ~BinaryOperator() override = default;
};

/// Prints all tuples from its input into the stream. Tuples are separated by a
/// newline character ("\n") and attributes are separated by a single comma
/// without any extra spaces. The last line also ends with a newline. Calling
/// `next()` prints the next tuple.
class Print : public UnaryOperator {
 private:
  // TODO: add your implementation here
  ostream& stream;
 public:
  Print(Operator& input, std::ostream& stream);     //一个注解： 因为这个input:Operator; Operator类是抽象类(无法生成实例)，所以需要Operator类的子类(base class)的实例作为输入。

  ~Print() override;

  void open() override;
  bool next() override;
  void close() override;
  std::vector<Register*> get_output() override;
};

/// Generates tuples from the input with only a subset of their attributes.
class Projection : public UnaryOperator {
 private:
  vector<size_t> attr_indexes;
  vector<Register*> output_tuple;

 public:
  Projection(Operator& input, std::vector<size_t> attr_indexes);  // 这是保留哪些attribute的index

  ~Projection() override;

  void open() override;
  bool next() override;
  void close() override;
  std::vector<Register*> get_output() override;
};

/// Filters tuples with the given predicate.
class Select : public UnaryOperator {
 public:
  enum class PredicateType {
    EQ,  // a == b
    NE,  // a != b
    LT,  // a < b
    LE,  // a <= b
    GT,  // a > b
    GE   // a >= b
  };
  enum class Select_Type { INT, STR, INNER};
  /// Predicate of the form:
  /// tuple[attr_index] P constant
  /// where P is given by `predicate_type`.
  struct PredicateAttributeInt64 {
    size_t attr_index;
    int64_t constant;
    PredicateType predicate_type;
  };

  /// Predicate of the form:
  /// tuple[attr_index] P constant
  /// where P is given by `predicate_type` and `constant` is a string of
  /// length 16.
  struct PredicateAttributeChar16 {
    size_t attr_index;
    std::string constant;
    PredicateType predicate_type;
  };

  /// tuple[attr_left_index] P tuple[attr_right_index]
  /// where P is given by `predicate_type`.
  struct PredicateAttributeAttribute {  //这个比较tuple内部的两个index位置值
    size_t attr_left_index;
    size_t attr_right_index;
    PredicateType predicate_type;
  };

 private:
  // three predicate, use in different SELECT situation.
  Select_Type type;
  PredicateAttributeInt64 num_predicate;
  PredicateAttributeChar16 str_predicate;
  PredicateAttributeAttribute inner_predicate;
  vector<Register*> output_tuple;

 public:
  Select(Operator& input, PredicateAttributeInt64 predicate);
  Select(Operator& input, PredicateAttributeChar16 predicate);
  Select(Operator& input, PredicateAttributeAttribute predicate);

  ~Select() override;

  void open() override;
  bool next() override;   // next return一个bool 告诉上一个operator后面有没有tuples继续。没有就return false;
  void close() override;
  std::vector<Register*> get_output() override;
};

/// Sorts the input by the given criteria.
class Sort : public UnaryOperator {
 public:
  struct Criterion {
    /// Attribute to be sorted.
    size_t attr_index;
    /// Sort descending?
    bool desc; // true就是从大到小，false就是从小到大
  };

  static bool compare_tuple(const vector<Register> tuple1,const vector<Register> tuple2, vector<Criterion> criteria ){
      if(tuple1[criteria[0].attr_index] != tuple2[criteria[0].attr_index]){
        bool desc = (tuple1[criteria[0].attr_index] > tuple2[criteria[0].attr_index]); // descending
        if(criteria[0].desc) return desc;
        else return !desc;
      }
      else{
        bool desc = (tuple1[criteria[1].attr_index] > tuple2[criteria[1].attr_index]); // descending
        if(criteria[1].desc) return desc;
        else return !desc;
      }
    }

 private:
  vector<Criterion> criteria;
  vector<vector<Register>> sort_tuples;
  size_t next_index;
  size_t sort_index;
  bool load;
 public:
  Sort(Operator& input, std::vector<Criterion> criteria);

  ~Sort() override;

  void open() override;
  bool next() override;
  void close() override;
  std::vector<Register*> get_output() override;
};

/// This can be used to store registers in an `std::unordered_map` or
/// `std::unordered_set`. Examples:
///
/// std::unordered_map<Register, int, RegisterHasher> map_from_reg_to_int;
/// std::unordered_set<Register, RegisterHasher> set_of_regs;
struct RegisterHasher {
  uint64_t operator()(const Register& r) const { return r.get_hash(); }
};

/// This can be used to store vectors of registers (which is how tuples are
/// represented) in an `std::unordered_map` or `std::unordered_set`. Examples:
///
/// std::unordered_map<std::vector<Register>, int, RegisterVectorHasher>
/// map_from_tuple_to_int; std::unordered_set<std::vector<Register>,
/// RegisterVectorHasher> set_of_tuples;
struct RegisterVectorHasher {
  uint64_t operator()(const std::vector<Register>& registers) const {
    std::string hash_values;
    for (auto& reg : registers) {
      uint64_t hash = reg.get_hash();
      hash_values.append(reinterpret_cast<char*>(&hash), sizeof(hash));
    }
    return std::hash<std::string>{}(hash_values);
  }
};

/// Computes the inner equi-join of the two inputs on one attribute.
class HashJoin : public BinaryOperator {
 private:
  size_t left_index;
  size_t right_index;
  bool join; 
  size_t next_index;
  size_t join_index;
  vector<vector<Register>> left_tuples;
  vector<vector<Register>> right_tuples;
  vector<vector<Register>> join_tuples;

 public:
  HashJoin(Operator& input_left, Operator& input_right, size_t attr_index_left,
           size_t attr_index_right);

  ~HashJoin() override;

  void open() override;
  bool next() override;
  void close() override;
  std::vector<Register*> get_output() override;
};

/// Groups and calculates (potentially multiple) aggregates on the input.
class HashAggregation : public UnaryOperator {
 public:
  /// Represents an aggregation function. For MIN, MAX, and SUM `attr_index`
  /// stands for the attribute which is being aggregated. For SUM the
  /// attribute must be in an `INT64` register.
  struct AggrFunc {
    enum Func { MIN, MAX, SUM, COUNT };

    Func func;
    size_t attr_index;
  };

 private:
  vector<AggrFunc> aggr_funcs;
  vector<size_t> group_by_attrs;
  size_t next_index;
  bool aggregate;
  bool min_max;
  Register Min_reg;
  Register Max_reg;
  vector<Register*> output_tuple;
  vector<vector<Register>> count_sum_tuples;
  unordered_map<int64_t, int64_t> sum_hash;
  unordered_map<int64_t, int64_t> count_hash;
 public:
  HashAggregation(Operator& input, std::vector<size_t> group_by_attrs,
                  std::vector<AggrFunc> aggr_funcs);

  ~HashAggregation() override;

  void open() override;
  bool next() override;
  void close() override;
  std::vector<Register*> get_output() override;
};

/// Computes the union of the two inputs with set semantics.
class Union : public BinaryOperator {
 private:
  unordered_set<int64_t> hash_table;
  vector<Register> Union_output; 
  bool load;
  size_t next_index;
  size_t union_index;
 public:
  Union(Operator& input_left, Operator& input_right);

  ~Union() override;

  void open() override;
  bool next() override;
  void close() override;
  std::vector<Register*> get_output() override;
};

/// Computes the union of the two inputs with bag semantics.
class UnionAll : public BinaryOperator {
 private:
  vector<Register> Union_all_output; 
  bool load;
  size_t next_index;
  size_t union_all_index;

 public:
  UnionAll(Operator& input_left, Operator& input_right);

  ~UnionAll() override;

  void open() override;
  bool next() override;
  void close() override;
  std::vector<Register*> get_output() override;
};

/// Computes the intersection of the two inputs with set semantics.
class Intersect : public BinaryOperator {
 private:
  unordered_set<uint64_t> hash_table;
  vector<Register> Intersect_output; 
  bool load;
  size_t next_index;
  size_t intersect_index;

 public:
  Intersect(Operator& input_left, Operator& input_right);

  ~Intersect() override;

  void open() override;
  bool next() override;
  void close() override;
  std::vector<Register*> get_output() override;
};

/// Computes the intersection of the two inputs with bag semantics.
class IntersectAll : public BinaryOperator {
 private:
  unordered_set<uint64_t> hash_table;
  vector<Register> Intersect_all_output; 
  bool load;
  size_t next_index;
  size_t intersect_all_index;

 public:
  IntersectAll(Operator& input_left, Operator& input_right);

  ~IntersectAll() override;

  void open() override;
  bool next() override;
  void close() override;
  std::vector<Register*> get_output() override;
};

/// Computes input_left - input_right with set semantics.
class Except : public BinaryOperator {
 private:
  unordered_set<uint64_t> hash_table;
  vector<Register> Except_output; 
  bool load;
  size_t next_index;
  size_t except_index;

 public:
  Except(Operator& input_left, Operator& input_right);

  ~Except() override;

  void open() override;
  bool next() override;
  void close() override;
  std::vector<Register*> get_output() override;
};

/// Computes input_left - input_right with bag semantics.
class ExceptAll : public BinaryOperator {
 private:
  vector<Register> Except_all_output; 
  bool load;
  size_t next_index;
  size_t except_all_index;

 public:
  ExceptAll(Operator& input_left, Operator& input_right);

  ~ExceptAll() override;

  void open() override;
  bool next() override;
  void close() override;
  std::vector<Register*> get_output() override;
};

}  // namespace operators
}  // namespace buzzdb
