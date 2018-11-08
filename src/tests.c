/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#include "parse_policies.h"
#include "minunit.h"
#include "compaction.h"
#include "buffer.h"
#include "rmutil/alloc.h"

MU_TEST(test_valid_policy) {
    SimpleCompactionRule* parsedRules;
    size_t rulesCount;
    int result = ParseCompactionPolicy("max:1m:1h;min:10s:5d:10d;last:5M:10ms;avg:2h:10d;avg:3d:100d", &parsedRules, &rulesCount);
	mu_check(result == TRUE);
	mu_check(rulesCount == 4);

    mu_check(parsedRules[0].aggType == StringAggTypeToEnum("max"));
    mu_assert_int_eq(parsedRules[0].timeBucket, 1);

    mu_check(parsedRules[1].aggType == StringAggTypeToEnum("min"));
    mu_check(parsedRules[1].timeBucket == 10*1000);

    mu_check(parsedRules[2].aggType == StringAggTypeToEnum("last"));
    mu_check(parsedRules[2].timeBucket == 5*60*1000);

    mu_check(parsedRules[3].aggType == StringAggTypeToEnum("avg"));
    mu_check(parsedRules[3].timeBucket == 2*60*60*1000);

    mu_check(parsedRules[4].aggType == StringAggTypeToEnum("avg"));
    mu_check(parsedRules[4].timeBucket == 3*60*60*1000*24);
    free(parsedRules);
}

MU_TEST(test_invalid_policy) {
    SimpleCompactionRule* parsedRules;
    size_t rulesCount;
    int result;
    result = ParseCompactionPolicy("max:1M;mins:10s;avg:2h;avg:1d", &parsedRules, &rulesCount);
	mu_check(result == FALSE);
	mu_check(rulesCount == 0);

    result = ParseCompactionPolicy("max:12hd;", &parsedRules, &rulesCount);
	mu_check(result == FALSE);
	mu_check(rulesCount == 0);
    free(parsedRules);
}

MU_TEST(test_buffer) {
    int data1 = 13;
    int data2 = 35; // 6 bits
    int result = 0;
    BitBuffer *buff = BitBuffer_new(1024);
    BitBuffer *reader = BitBuffer_newWithData(1024, buff->data);
    for (int i = 0; i < 20; i++)
    {
        BitBuffer_write (buff, data1 << 4, 4);
        BitBuffer_write (buff, data2 << 2, 6);
    }
    
    for (int i = 0; i < 20; i++)
    {
        result = BitBuffer_read(reader, 4);
        mu_check(result == data1);

        result = BitBuffer_read(reader, 6);
        mu_check(result == data2);
    }
    BitBuffer_free(buff);
}

MU_TEST(test_buffer2) {
    int data = 35; // 6 bits
    int result = 0;
    BitBuffer *buff = BitBuffer_new(1024);
    BitBuffer *reader = BitBuffer_newWithData(1024, buff->data);
    BitBuffer_write (buff, data << 2, 6);
    result = BitBuffer_read(reader, 6);
    mu_check(result == data);
    BitBuffer_free(buff);
}

MU_TEST(test_StringLenAggTypeToEnum) {
    mu_check(StringAggTypeToEnum("min") == TS_AGG_MIN);
    mu_check(StringAggTypeToEnum("max") == TS_AGG_MAX);
    mu_check(StringAggTypeToEnum("sum") == TS_AGG_SUM);
    mu_check(StringAggTypeToEnum("avg") == TS_AGG_AVG);
    mu_check(StringAggTypeToEnum("count") == TS_AGG_COUNT);
    mu_check(StringAggTypeToEnum("first") == TS_AGG_FIRST);
    mu_check(StringAggTypeToEnum("last") == TS_AGG_LAST);
    mu_check(StringAggTypeToEnum("range") == TS_AGG_RANGE);
}

MU_TEST_SUITE(test_suite) {
	MU_RUN_TEST(test_valid_policy);
	MU_RUN_TEST(test_invalid_policy);
    MU_RUN_TEST(test_StringLenAggTypeToEnum);
	MU_RUN_TEST(test_buffer);
    MU_RUN_TEST(test_buffer2);
}

int main(int argc, char *argv[]) {
    RMUTil_InitAlloc();
    MU_RUN_SUITE(test_suite);
	MU_REPORT();
	return 0;
}