export SECONDARY_DB_PREFIX=r1
export TERTIARY_DB_PREFIX=cdb2

export CHECK_DB_AT_FINISH=0
ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif

ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=15m
endif

unexport CLUSTER
