package com.aliyun.dts.subscribe.clients.filter.utils;

import com.aliyun.dts.subscribe.clients.exception.CriticalException;
import com.aliyun.dts.subscribe.clients.filter.DataFilter;
import com.aliyun.dts.subscribe.clients.filter.DataFilterImpl;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DataFilterImplTest {

    String specialString = "ju_seller;seller;*|ju_seller;mer_activity_sign_info;*|ju_seller;mer_activity_materail;*|ju_seller;mer_activity_sign_expand;*|ju_seller;sign_record_extra;*|ju_seller;floor_config;*";

    String ttFilterString = "*.CFM_BIZ_TRANS_INFO_[0-9]*.*. ..|"
            + "*.CFM_BIZ_TRANS_PROP_[0-9]*.*|"
            + "*.CFM_FUND_ORDER_[0-9]*.a.b.c|"
            + "hello.CFM_FUND_ORDER_TERM_[0-9]*.sd.*.2|"
            + "*.CFM_PAYMENT_CARD_[0-9]*.*|"
            + "ooo .CFM_RELATED_PARTY_[0-9]*.sdf|"
            + " hhh.CP_BILL_DETAIL_[0-9]*.a. ds .d|"
            + " mmm .CP_COUPON_[0-9]*.s. dsf.d|"
            + "*.CP_TEMPLATE_[0-9]*. sdd.sd|"
            + "*.CP_TEMPLATE_BILL_STATA_[0-9]*.*|"
            + "*.FC_FN_INSTRUCTION_[0-9]*.*|"
            + "*.FC_FN_PMT_ORDER_[0-9]*.*|"
            + "*.FC_MF_DP_ORDER_[0-9]*.*|"
            + "*.FC_MF_FZ_ORDER_[0-9]*. * |"
            + "*.FC_MF_INSTRUCTION_[0-9]*. *|"
            + "*.FC_MF_RD_ORDER_[0-9]*.* |"
            + "*.FMP_USER_ACTIVITY_CONTROL_[0-9]*.sdf|"
            + "*.FMP_YEB_SIGN_SUBCARD_[0-9]*.*|";

    String ttOrgFilterString = "*.CFM_BIZ_TRANS_INFO_[0-9]*.*|"
            + "*.CFM_BIZ_TRANS_PROP_[0-9]*.*|"
            + "*.CFM_FUND_ORDER_[0-9]*.*|"
            + "*.CFM_FUND_ORDER_TERM_[0-9]*.*|"
            + "*.CFM_PAYMENT_CARD_[0-9]*.*|"
            + "*.CFM_RELATED_PARTY_[0-9]*.*|"
            + "*.CP_BILL_DETAIL_[0-9]*.*|"
            + "*.CP_COUPON_[0-9]*.*|"
            + "*.CP_TEMPLATE_[0-9]*.*|"
            + "*.CP_TEMPLATE_BILL_STATA_[0-9]*.*|"
            + "*.FC_FN_INSTRUCTION_[0-9]*.*|"
            + "*.FC_FN_PMT_ORDER_[0-9]*.*|"
            + "*.FC_MF_DP_ORDER_[0-9]*.*|"
            + "*.FC_MF_FZ_ORDER_[0-9]*.*|"
            + "*.FC_MF_INSTRUCTION_[0-9]*.*|"
            + "*.FC_MF_RD_ORDER_[0-9]*.*|"
            + "*.FMP_USER_ACTIVITY_CONTROL_[0-9]*.*|"
            + "*.FMP_YEB_SIGN_SUBCARD_[0-9]*.*|"
            + "*.FMP_YEB_TRANS_IN_ORDER_[0-9]*.*|"
            + "*.FMP_YEB_TRANS_OUT_ORDER_[0-9]*.*|"
            + "*.FMP_YEB_USER_BIND_[0-9]*.*|"
            + "*.IW_ACCOUNT_LOG_[0-9]*.*|"
            + "*.IW_TRANS_LOG_[0-9]*.*|"
            + "*.MC_SETTLE_BILL_[0-9]*.*|"
            + "*.MC_SETTLE_COMMON_REQ_[0-9]*.*|"
            + "*.MC_SETTLE_ENGINE_[0-9]*.*|"
            + "*.MC_SETTLE_ITEM_[0-9]*.*|"
            + "*.MOA_SHOP_ORDER_EXTEND_[0-9]*.*|"
            + "*.PCB_BILL_DAILY_[0-9]*.*|"
            + "*.PCB_BILL_[0-9]*.*|"
            + "*.PCB_BILL_LOG_[0-9]*.*|"
            + "*.PCB_COMMON_BILL_[0-9]*.*|"
            + "*.PCB_COMMON_BILL_LOG_[0-9]*.*|"
            + "*.PCB_INSTALLMENT_CTRL_[0-9]*.*|"
            + "*.PCB_INSTALLMENT_[0-9]*.*|"
            + "*.PCB_INSTALLMENT_LOG_[0-9]*.*|"
            + "*.PCB_INSTALLMENT_TRANS_[0-9]*.*|"
            + "*.PCB_INTEREST_ACCUM_[0-9]*.*|"
            + "*.PCB_INTEREST_LOG_[0-9]*.*|"
            + "*.PCB_STATEMENT_[0-9]*.*|"
            + "*.PCB_TRANS_LOG_[0-9]*.*|"
            + "*.PCC_AMOUNT_OCCUPY_ORDER_[0-9]*.*|"
            + "*.PCC_CARD_LOG_[0-9]*.*|"
            + "*.PCC_CASHIER_ORDER_[0-9]*.*|"
            + "*.PCC_CHARGE_ORDER_[0-9]*.*|"
            + "*.PCC_CONFIRM_ORDER_[0-9]*.*|"
            + "*.PCC_CREDIT_CARD_[0-9]*.*|"
            + "*.PCC_FACTOR_ORDER_[0-9]*.*|"
            + "*.PCC_FUND_ACCOUNT_[0-9]*.*|"
            + "*.PCC_LOAN_ORDER_[0-9]*.*|"
            + "*.PCC_LOAN_POST_ORDER_[0-9]*.*|"
            + "*.PCC_PAYMENT_ORDER_[0-9]*.*|"
            + "*.PCC_PLEDGE_ORDER_[0-9]*.*|"
            + "*.PCC_REFUND_ORDER_[0-9]*.*|"
            + "*.PCC_REPAY_DETAIL_[0-9]*.*|"
            + "*.PCC_REPAY_DETAIL_INSTALL_[0-9]*.*|"
            + "*.PCC_REPAY_ORDER_[0-9]*.*|"
            + "*.SF_SIGN_CONTRACT_[0-9]*.*";

    static String dbAndtables[][] =  {{"hello", "CFM_BIZ_TRANS_INFO_123"},
            {"hhh", "CFM_BIZ_TRANS_PROP_2332"},
            {"hhh", "CFM_FUND_ORDER_3242"},
            {"hello", "CFM_FUND_ORDER_TERM_234243"},
            {"sdfsdf", "CFM_PAYMENT_CARD_4444"},
            {"ooo ", "CFM_RELATED_PARTY_23423"},
            {" hhh", "CP_BILL_DETAIL_32432"},
            {" mmm ", "CP_COUPON_234234"},
            {"dfdsf", "CP_TEMPLATE_234234"},
            {"dfsf", "CP_TEMPLATE_BILL_STATA_4444"},
            {"sss", "FC_FN_INSTRUCTION_2332"},
            {"sdfsdf", "FC_FN_PMT_ORDER_243"},
            {"fdsfsdf", "FC_MF_DP_ORDER_23232"},
            {"23423", "FC_MF_FZ_ORDER_3232"},
            {"fsdf", "FC_MF_INSTRUCTION_8765"},
            {"fsdfsdf", "FC_MF_RD_ORDER_23423"},
            {"kjhg", "FMP_USER_ACTIVITY_CONTROL_5543"},
            {"ghfd", "FMP_YEB_SIGN_SUBCARD_543"},
            {"sfdsfd", "FMP_YEB_TRANS_IN_ORDER_4443"},
            {"sfdsf", "FMP_YEB_TRANS_OUT_ORDER_345"},
            {"ggfff", "FMP_YEB_USER_BIND_34345"},
            {"gfsd", "IW_ACCOUNT_LOG_5423"},
            {"dssdf", "IW_TRANS_LOG_5434"},
            {"hdfsf", "MC_SETTLE_BILL_5454"},
            {"gsdfs", "MC_SETTLE_COMMON_REQ_4353"},
            {"fdsfs", "MC_SETTLE_ENGINE_643534"},
            {"gdfas", "MC_SETTLE_ITEM_45"},
            {"fsd", "MOA_SHOP_ORDER_EXTEND_5435"},
            {"dfs2", "PCB_BILL_DAILY_543"},
            {"dfsf", "PCB_BILL_3242"},
            {"fdsaf", "PCB_BILL_LOG_243"},
            {"fdsfs", "PCB_COMMON_BILL_2342"},
            {"fsdfsd", "PCB_COMMON_BILL_LOG_423"},
            {"fsdf", "PCB_INSTALLMENT_CTRL_234"},
            {"rgad", "PCB_INSTALLMENT_543"},
            {"hjy", "PCB_INSTALLMENT_LOG_43"},
            {"joifds", "PCB_INSTALLMENT_TRANS_24"},
            {"mnhvd", "PCB_INTEREST_ACCUM_234"},
            {"poih", "PCB_INTEREST_LOG_534"},
            {"poc", "PCB_STATEMENT_234"},
            {"weerw", "PCB_TRANS_LOG_987"},
            {"pqwd", "PCC_AMOUNT_OCCUPY_ORDER_9475"},
            {"df2", "PCC_CARD_LOG_23"},
            {"okkv", "PCC_CASHIER_ORDER_87"},
            {"hgsdf", "PCC_CHARGE_ORDER_23"},
            {"fds", "PCC_CONFIRM_ORDER_12"},
            {"fsdf", "PCC_CREDIT_CARD_112"},
            {"qwcs", "PCC_FACTOR_ORDER_23"},
            {"qpovd", "PCC_FUND_ACCOUNT_24"},
            {"oiuqnw", "PCC_LOAN_ORDER_034"},
            {"fdsfgsgd", "PCC_LOAN_POST_ORDER_98"},
            {"oihv", "PCC_PAYMENT_ORDER_88"},
            {"csidjc", "PCC_PLEDGE_ORDER_980"},
            {"qpnd", "PCC_REFUND_ORDER_98"},
            {"sdf12", "PCC_REPAY_DETAIL_123"},
            {"pkokfd", "PCC_REPAY_DETAIL_INSTALL_132"},
            {"pposj", "PCC_REPAY_ORDER_1203"},
            {"pnvnud", "SF_SIGN_CONTRACT_10"}};

    public DataFilter buildDataFilterFromString(String filter, String innerSpiltChar, String splitChar, boolean isOB, String branchDB) {
        String[] splitString = filter.split(splitChar);
        DataFilterImpl dataFilterBase = DataFilterImpl.create();
        for (String pair : splitString) {
            String[] tmp = pair.split(innerSpiltChar);
            int colsOffset = isOB ? 3 : 2;
            String[] colsArray = new String[tmp.length  - colsOffset];
            for (int i = colsOffset; i < tmp.length; ++i) {
                colsArray[i - colsOffset] = tmp[i];
            }
            if (isOB) {
                dataFilterBase.addFilterTuple(tmp[0], tmp[1], tmp[2], colsArray);
            } else {
                dataFilterBase.addFilterTuple(null, tmp[0], tmp[1], colsArray);
            }
        }

        dataFilterBase.validateFilter();
        return dataFilterBase;
    }

    @Test
    public void testFilterPerformance() {
        DataFilter filter = buildDataFilterFromString(ttOrgFilterString, "\\.", "\\|", false, null);
//        filter.validateFilter(DBType.MYSQL);
        assertTrue(filter.getIsAllMatch() == true);
        int randomSize = dbAndtables.length;
        Random rand = new Random();
        List<Integer> cachedOrder = new ArrayList<Integer>();

        for(int i = 0; i < 100000; ++i) {
            cachedOrder.add(rand.nextInt(100) % randomSize);
        }
        long currentTime = System.currentTimeMillis();
        for(Integer i : cachedOrder) {
            List<String> str = DataFilterUtil.getColNames(dbAndtables[i][0], dbAndtables[i][1], filter);
            assertTrue(null != str && str.size() == 1);
        }
        long afterDoneTime = System.currentTimeMillis();
        System.out.println("100w filter without accu cost " + (afterDoneTime - currentTime));

        currentTime = System.currentTimeMillis();
        for(Integer i : cachedOrder) {
            List<String> str = DataFilterUtil.getColNamesWithMapping(dbAndtables[i][0], dbAndtables[i][1], filter);
            assertTrue(null != str && str.size() == 1);
        }
        afterDoneTime = System.currentTimeMillis();
        System.out.println("100w filter accued  cost " + (afterDoneTime - currentTime));
    }


    @Test
    public void testFilterCompatible() {
        DataFilter filter = buildDataFilterFromString(ttFilterString, "\\.", "\\|", false, null);
        filter.toString();
        List<String> toFindCols = DataFilterUtil.getColNames("hello", "CFM_FUND_ORDER_TERM_111", filter);
        assertTrue(toFindCols != null);
//		System.out.println(toFindCols.toString());
        assertTrue(DataFilterUtil.isColInArray("asdf", toFindCols) && DataFilterUtil.isColInArray("sd", toFindCols));
        toFindCols = DataFilterUtil.getColNames("ooo", "CFM_RELATED_PARTY_323", filter);
        //not support trim
//		System.out.println(toFindCols);
        toFindCols = DataFilterUtil.getColNames("hhh", "CP_BILL_DETAIL_232", filter);
//		System.out.println(toFindCols);
        toFindCols = DataFilterUtil.getColNames("ooo ", "CFM_RELATED_PARTY_323", filter);
//		System.out.println(toFindCols);
        toFindCols = DataFilterUtil.getColNames("ooo ", "FC_MF_INSTRUCTION_234", filter);
//		System.out.println(toFindCols);
        assertFalse(DataFilterUtil.isColInArray("asdf", toFindCols));
        assertTrue(DataFilterUtil.isColInArray(" *", toFindCols));
        toFindCols = DataFilterUtil.getColNames("ooo ", "FC_MF_RD_ORDER_234243", filter);
//		System.out.println(toFindCols);
        assertFalse(DataFilterUtil.isColInArray("asdf", toFindCols));
        assertFalse(DataFilterUtil.isColInArray(" *", toFindCols));
        assertTrue(DataFilterUtil.isColInArray("* ", toFindCols));
//		System.out.println(toStoresFilter);
        toFindCols = DataFilterUtil.getColNames("ooo ", "FC_MF_FZ_ORDER_2343", filter);
//		System.out.println(toFindCols);
        assertFalse(DataFilterUtil.isColInArray("asdf", toFindCols));
        assertFalse(DataFilterUtil.isColInArray(" *", toFindCols));
        assertFalse(DataFilterUtil.isColInArray("* ", toFindCols));
        assertTrue(DataFilterUtil.isColInArray(" * ", toFindCols));

        for(int i = 0; i < 18; ++i) {
            String[] pairs = dbAndtables[i];
            assertTrue(pairs.length == 2);
            List<String> cols = DataFilterUtil.getColNames(pairs[0], pairs[1], filter);
            assertTrue(cols != null);
//			System.out.println(cols.toString());
        }
        assertTrue(filter.getIsAllMatch() == false);
    }

    @Test
    public void testDataFilterPut() {
        DataFilter implem = DataFilterImpl.create().addFilterTuple("ttt", "aa", "bb", "c1", "c2", "c3", "c4")
                .addFilterTuple("ttt1", "aa1", "bb1", "cc1", "cc2", "cc3");
        List<DataFilterImpl.FilterInfo> filterInfoList = ((DataFilterImpl)implem).getFilterInfoList();
        assertTrue(filterInfoList.size() == 2);
        for (DataFilterImpl.FilterInfo filterInfo : filterInfoList) {
            if (StringUtils.equals(filterInfo.getTableName(), "bb")) {
                assertTrue(StringUtils.equals("ttt", filterInfo.getTenant()));
                assertTrue(StringUtils.equals("aa", filterInfo.getDbName()));
                assertTrue(filterInfo.getColsList().size() == 4);
                System.out.println(StringUtils.join(filterInfo.getColsList(), ","));
            } else {
                assertTrue(StringUtils.equals("aa1", filterInfo.getDbName()));
                assertTrue(StringUtils.equals("bb1", filterInfo.getTableName()));
                assertTrue(StringUtils.equals("ttt1", filterInfo.getTenant()));
                assertTrue(filterInfo.getColsList().size() == 3);
                System.out.println(StringUtils.join(filterInfo.getColsList(), ","));
            }
        }
        assertTrue(null == implem.getConnectStoreFilterConditions() && null == implem.toString());
        boolean validateResult = implem.validateFilter();
        assertTrue(validateResult);
        assertTrue(StringUtils.equals("aa.bb|aa1.bb1|", implem.getConnectStoreFilterConditions()) &&
                StringUtils.equals("aa.bb|aa1.bb1|", implem.toString()));
        List<String> cols = DataFilterUtil.getColNamesWithMapping("aa", null, implem);
        assertTrue(null == cols);
        cols = DataFilterUtil.getColNamesWithMapping("aa", "bb", implem);
        assertTrue(cols.size() == 4 && cols.contains("c1") && cols.contains("c2")
                && cols.contains("c3") && cols.contains("c4"));
        assertTrue(implem.getIsAllMatch() == false);

    }
    @Test
    public void testBranchDBFilter() {
        DataFilter implem = DataFilterImpl.create().addFilterTuple("ttt", "aa", "bb", "c1", "c2", "c3", "c4")
                .addFilterTuple("ttt1", "aa1", "bb1", "cc1", "cc2", "cc3");
        assertTrue(null == implem.getConnectStoreFilterConditions() && null == implem.toString());
        boolean validateResult = implem.validateFilter();
        assertTrue(validateResult);
        assertTrue(StringUtils.equals("aa.bb|aa1.bb1|", implem.getConnectStoreFilterConditions()) &&
                StringUtils.equals("aa.bb|aa1.bb1|", implem.toString()));
        List<String> cols = DataFilterUtil.getColNamesWithMapping("aa", null, implem);
        assertTrue(null == cols);
        cols = DataFilterUtil.getColNamesWithMapping("xx1024xx", "bb", implem);
        assertTrue(null == cols);
        cols = DataFilterUtil.getColNamesWithMapping("aa", "bb", implem);
        assertTrue(cols.size() == 4 && cols.contains("c1") && cols.contains("c2")
                && cols.contains("c3") && cols.contains("c4"));
        cols = DataFilterUtil.getColNamesWithMapping("aa1", "bb1", implem);
        assertTrue(cols.size() == 3 && cols.contains("cc1") && cols.contains("cc2")
                && cols.contains("cc3"));
        assertTrue(implem.getIsAllMatch() == false);
    }

    @Test
    public void testErrorCase() {
        DataFilter implem = DataFilterImpl.create();
        try {
            boolean validateResult = implem.validateFilter();
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(true);
            assertTrue(e instanceof CriticalException);
            assertTrue(e.getMessage().contains("Filter list is empty, use addFilterTuple add filter tuple"));
            System.out.println(e.getMessage());
        }
        implem = DataFilterImpl.create().addFilterTuple("xx", "aa", "bb", "cc").addFilterTuple(null, "a1", "b1", "c1");
        try {
            boolean validateResult = implem.validateFilter();
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(true);
            assertTrue(e instanceof CriticalException);
            assertTrue(e.getMessage().contains("Target database is OB1.0, tenant is strictly required"));
            System.out.println(e.getMessage());
        }
        implem = DataFilterImpl.create().addFilterTuple("xx", "aa", "bb", "cc").addFilterTuple("xx", "a1", "b1");
        try {
            boolean validateResult = implem.validateFilter();
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(true);
            assertTrue(e instanceof CriticalException);
            assertTrue(e.getMessage().contains("Col filter must be set, Current filter tuple"));
            System.out.println(e.getMessage());
        }
        implem = DataFilterImpl.create().addFilterTuple("xx", null, "bb", "cc").addFilterTuple("xx", "a1", "b1", "cc");
        try {
            boolean validateResult = implem.validateFilter();
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(true);
            assertTrue(e instanceof CriticalException);
            assertTrue(e.getMessage().contains("DBName and TableName is strictly required, Current filter tuple"));
            System.out.println(e.getMessage());
        }
        implem = DataFilterImpl.create().addFilterTuple("xx", "aa", "bb", "cc").addFilterTuple("xx", "a1", null, "cc");
        try {
            boolean validateResult = implem.validateFilter();
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(true);
            assertTrue(e instanceof CriticalException);
            assertTrue(e.getMessage().contains("DBName and TableName is strictly required, Current filter tuple"));
            System.out.println(e.getMessage());
        }
    }
}
