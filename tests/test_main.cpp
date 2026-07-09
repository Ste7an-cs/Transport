#include <QCoreApplication>
#include <gtest/gtest.h>

// 传输测试需 Qt 事件循环:进程级 QCoreApplication(不 exec,靠 processEvents 泵)。
int main(int argc, char** argv) {
  QCoreApplication app(argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
