cmake_minimum_required(VERSION 3.16)

project(binance_capture LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(MSVC)
  add_compile_options(/W4)
else()
  add_compile_options(-Wall -Wextra -O2)
endif()

find_package(Boost REQUIRED COMPONENTS system thread)
find_package(OpenSSL REQUIRED)
find_package(nlohmann_json 3.2 REQUIRED)

include_directories(include)

add_executable(binance_capture src/main.cpp src/binance_capture.cpp)
target_include_directories(binance_capture PRIVATE include)
target_link_libraries(binance_capture PRIVATE OpenSSL::SSL OpenSSL::Crypto Boost::system Boost::thread nlohmann_json::nlohmann_json ws2_32)

add_executable(order_book_tests tests/test_order_book.cpp src/binance_capture.cpp)
target_include_directories(order_book_tests PRIVATE include)
target_link_libraries(order_book_tests PRIVATE OpenSSL::SSL OpenSSL::Crypto Boost::system Boost::thread nlohmann_json::nlohmann_json ws2_32)

enable_testing()
add_test(NAME order_book_tests COMMAND order_book_tests)
