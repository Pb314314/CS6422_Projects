#include <gtest/gtest.h>

#include "tutorial/tutorial.h"


namespace {

using buzzdb::tutorial::CommandExecutor;


using namespace std;
TEST(TutorialTest, ShouldReturnCorrectLocationOnLoadingData) {
  CommandExecutor commandExecutor;
  commandExecutor.execute("load data/sample.txt");
  EXPECT_EQ(commandExecutor.execute("locate song 1"), "3");
  EXPECT_EQ(commandExecutor.execute("locate       soNg        1 "), "3");
  EXPECT_EQ(commandExecutor.execute(" locate       SoNg        1 "), "3");
  EXPECT_EQ(commandExecutor.execute(" locate       pie        1 "), "18");
  EXPECT_EQ(commandExecutor.execute(" loCAte       pie        2 "), "21");
  EXPECT_EQ(commandExecutor.execute(" locate       pie        3 "),
            "No matching entry");
  EXPECT_EQ(commandExecutor.execute(" locate       prince        "),
            "ERROR: Invalid command");
  commandExecutor.execute("new");
  EXPECT_EQ(commandExecutor.execute("locate       soNg        1 "),
            "No matching entry");
}

TEST(TutorialTest, ShouldClearTreeOnNewCommand) {
  CommandExecutor commandExecutor;
  commandExecutor.execute("load data/sample.txt");
  EXPECT_EQ(commandExecutor.execute(" loCAte       pie        2 "), "21");
  commandExecutor.execute("new");
  EXPECT_EQ(commandExecutor.execute("locate       soNg        1 "),
            "No matching entry");
}

TEST(TutorialTest, ShouldReturnInvalidForAllInvalidQueries) {
  CommandExecutor commandExecutor;
  EXPECT_EQ(commandExecutor.execute("locate       so#Ng        1 "),
            "ERROR: Invalid command");
  EXPECT_EQ(commandExecutor.execute(" find       SoNg        1 "),
            "ERROR: Invalid command");
  EXPECT_EQ(commandExecutor.execute(" locate       pie1"),
            "ERROR: Invalid command");
}

TEST(TutorialTest, ShouldWorkAfterReloading) {
  CommandExecutor commandExecutor;
  commandExecutor.execute("      load      data/sample.txt      ");
  EXPECT_EQ(commandExecutor.execute("locate song 1"), "3");
  EXPECT_EQ(commandExecutor.execute("locate       soNg        1 "), "3");
  EXPECT_EQ(commandExecutor.execute(" locate       SoNg        1 "), "3");
  EXPECT_EQ(commandExecutor.execute(" locate       pie        1 "), "18");
  EXPECT_EQ(commandExecutor.execute(" loCAte       pie        0000000000002 "),
            "21");
  EXPECT_EQ(commandExecutor.execute(" LOCATE       pie        3 "),
            "No matching entry");
  EXPECT_EQ(commandExecutor.execute(" locate       prince        "),
            "ERROR: Invalid command");
  commandExecutor.execute("      new   ");
  EXPECT_EQ(commandExecutor.execute("locate       soNg               1 "),
            "No matching entry");
  commandExecutor.execute("      load      data/sample.txt      ");
  EXPECT_EQ(commandExecutor.execute("locate       soNg        1 "), "3");
  EXPECT_EQ(commandExecutor.execute(" locate       pie        1 "), "18");
}

TEST(TutorialTest, ShouldWorkForBiggerFiles) {
  CommandExecutor commandExecutor;
  commandExecutor.execute("      load      data/wrnpc.txt      ");
  EXPECT_EQ(commandExecutor.execute("locate      Tolstoy  15"),
            "No matching entry");
  EXPECT_EQ(commandExecutor.execute("locate      tolstoy  1"), "12");
  EXPECT_EQ(commandExecutor.execute("locate       soNg        1 "), "30772");
  EXPECT_EQ(commandExecutor.execute(" locate       SoNg        1 "), "30772");
  EXPECT_EQ(commandExecutor.execute(" locate       pie        3 "),
            "No matching entry");
  EXPECT_EQ(commandExecutor.execute(" locate prince "),
            "ERROR: Invalid command");
  commandExecutor.execute("      new   ");
  EXPECT_EQ(commandExecutor.execute("locate       soNg        1 "),
            "No matching entry");
  commandExecutor.execute("      load      data/sample.txt ");
  EXPECT_EQ(commandExecutor.execute("locate       soNg        1 "), "3");
}

TEST(TestSuit, ShouldGiveInvalidResponseForAllInvalidQueries) {
  CommandExecutor commandExecutor;
  EXPECT_EQ(commandExecutor.execute("locate       so#Ng        1 "),
            "ERROR: Invalid command");
  EXPECT_EQ(commandExecutor.execute("locate       soNg        -1 "),
            "ERROR: Invalid command");
  EXPECT_EQ(commandExecutor.execute("locate       soNg        1. "),
            "ERROR: Invalid command");
  EXPECT_EQ(commandExecutor.execute(" find       SoNg        1 "),
            "ERROR: Invalid command");
  EXPECT_EQ(commandExecutor.execute(" find       SoNg        0 "),
            "ERROR: Invalid command");
  EXPECT_EQ(commandExecutor.execute(" locate       pie1"),
            "ERROR: Invalid command");
  EXPECT_EQ(commandExecutor.execute("| locate  |     pie1              10"),
            "ERROR: Invalid command");
  EXPECT_EQ(commandExecutor.execute("| locate  |     pie1              10"),
            "ERROR: Invalid command");
  EXPECT_EQ(commandExecutor.execute("locate     pie_1              10"),
            "ERROR: Invalid command");
}

}  // namespace

int main(int argc, char* argv[]) {
  //cout<< " My Test:"<<endl;
  //Btree c;
  //c.A_test();
  //string com = "      LOAD      data/sample.txt      ";
  //CommandExecutor command_test;
  //command_test.execute(com);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
