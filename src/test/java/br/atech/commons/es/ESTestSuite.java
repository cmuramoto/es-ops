package br.atech.commons.es;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@SuiteClasses({ //
		TestAggregation.class, TestBoolean.class, TestBounded.class, TestBulkInsert.class, TestCreateDefinition.class, TestDeleteByTTL.class, //
		TestKeywordThenText.class, TestNGramThenText.class, TestPaging.class, TestQuery.class, TestQueryRangeWithScript.class, TestQueryWithDefaults.class, //
		TestRestClient.class, TestScroll.class, TestSelfAdjustingBulkInsert.class, TestStatefullScroll.class, TestVersion.class//
})
@RunWith(Suite.class)
public class ESTestSuite {

}
