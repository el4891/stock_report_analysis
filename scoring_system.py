#!/usr/bin/python3
import os

from datetime import datetime
import pandas as pd
import tushare as ts
from stock_analysis.stock_sum import stock_sum

out_folder = os.path.dirname(os.path.abspath(__file__)) + '/out'
stock_report_dir = os.path.dirname(os.path.abspath(__file__)) + '/finance'

s_sum = None

calcu_end_year = 2016  # 计算平均利润的截止年,包括该年
month_day = '-12-31'
max_pe = 20

DEBUG = True

today = str(datetime.now())[:10]


def pianyi_func(data, year):

    for i in range(year - 2, year + 1):
        data = data[data['经营活动产生的现金流量净额(万元)' + str(i)] > 10000]
        data = data[data['净利润(万元)' + str(i)] > 10000]

    data = data[data['应收款(万元)' + str(year)] / data['资产总计(万元)' + str(year)] < 0.4]
    data = data[data['货币资金(万元)' + str(year)] + (data['流动资产合计(万元)' + str(year)] - data['货币资金(万元)' + str(year)]) / 5 > data['负债合计(万元)' + str(year)]]

    data = data[data['净利润增长率(%)'+ str(year)] + data['净利润增长率(%)'+ str(year - 1)] + data['净利润增长率(%)'+ str(year - 2)] > 30]
    data = data[(data['经营活动产生的现金流量净额(万元)' + str(year)] + data['经营活动产生的现金流量净额(万元)' + str(year - 1)] + data['经营活动产生的现金流量净额(万元)' + str(year - 2)] + data['经营活动产生的现金流量净额(万元)' + str(year - 3)]) / (data['净利润(万元)' + str(year)] + data['净利润(万元)' + str(year - 1)] + data['净利润(万元)' + str(year - 2)] + data['净利润(万元)' + str(year - 3)]) > 1.2]
    data = data[data['毛利率(%)' + str(year)] + data['毛利率(%)' + str(year - 1)] + data['毛利率(%)' + str(year - 2)] + data['毛利率(%)' + str(year - 3)] > 40]

    data['平均市净率'] = data['价格'] / ((data['流动资产合计(万元)' + str(year)]  - data['负债合计(万元)' + str(year)]) / data['总股本'] / 10000)

    data['每股平均利润'] = data['每股平均利润'].round(3)

    data['阈值净利率'] = (data['净利润增长率(%)' + str(year)] * 1.4 + data['净利润增长率(%)' + str(year - 1)] * 1.2 + data[
        '净利润增长率(%)' + str(year - 2)]) / 13
    data['阈值净利率'] = data['阈值净利率'].round(1)

    data['阈值毛利率'] = (data['毛利率(%)' + str(year)] * 1.2 + data['毛利率(%)' + str(year - 1)] * 1.1 + data[
        '毛利率(%)' + str(year - 2)]) / 16
    data['阈值毛利率'] = data['阈值毛利率'].round(1)

    data['阈值市盈率'] = 5 + data['阈值净利率'] + data['阈值毛利率']
    data['阈值市盈率'] = data['阈值市盈率'].round(1)

    data = data[data['平均市盈率'] < 21]
    data = data[data['平均市净率'] < 3]

    data = data[['名字', '行业', '地区', '每股平均利润', '阈值市盈率', '价格', '平均市盈率', '平均市净率']]

    data.to_excel(os.path.join(out_folder, '%s便宜股票.xlsx' % (today)))

def operation_func(data, year):
    data['每股平均利润'] = data['每股平均利润'].round(3)

    data['阈值净利率'] = (data['净利润增长率(%)' + str(year)] * 1.4 + data['净利润增长率(%)' + str(year - 1)] * 1.2 + data['净利润增长率(%)' + str(year - 2)]) / 13
    data['阈值净利率'] = data['阈值净利率'].round(1)

    data['阈值毛利率'] = (data['毛利率(%)' + str(year)] * 1.2 + data['毛利率(%)' + str(year - 1)] * 1.1 + data['毛利率(%)' + str(year - 2)]) / 16
    data['阈值毛利率'] = data['阈值毛利率'].round(1)

    data['阈值市盈率'] = 5 + data['阈值净利率'] + data['阈值毛利率']
    data['阈值市盈率'] = data['阈值市盈率'].round(1)

    data['静观其变'] = data['阈值市盈率'] * data['每股平均利润']
    data['静观其变'] = data['静观其变'].round(1)
    
    data['买入六份'] = data['静观其变'] * 0.5
    data['买入六份'] = data['买入六份'].round(1)

    data['买入四份'] = data['静观其变'] * 0.6
    data['买入四份'] = data['买入四份'].round(1)

    data['买入三份'] = data['静观其变'] * 0.7
    data['买入三份'] = data['买入三份'].round(1)

    data['买入两份'] = data['静观其变'] * 0.8
    data['买入两份'] = data['买入两份'].round(1)

    data['买入一份'] = data['静观其变'] * 0.9
    data['买入一份'] = data['买入一份'].round(1)

    data['卖出一份'] = data['静观其变'] * 1.1
    data['卖出一份'] = data['卖出一份'].round(1)

    data['卖出两份'] = data['静观其变'] * 1.2
    data['卖出两份'] = data['卖出两份'].round(1)

    data['卖出三份'] = data['静观其变'] * 1.3
    data['卖出三份'] = data['卖出三份'].round(1)

    data['卖出四份'] = data['静观其变'] * 1.4
    data['卖出四份'] = data['卖出四份'].round(1)

    data['卖出六份'] = data['静观其变'] * 1.5
    data['卖出六份'] = data['卖出六份'].round(1)

    data['卖出八份'] = data['静观其变'] * 1.6
    data['卖出八份'] = data['卖出八份'].round(1)

    data['二十倍市盈率'] = data['每股平均利润'] * 20
    data['二十倍市盈率'] = data['二十倍市盈率'].round(1)

    data = data[['名字', '行业', '地区', '每股平均利润', '阈值市盈率', '阈值净利率', '阈值毛利率', '价格', '二十倍市盈率',
                 '买入六份', '买入四份', '买入三份', '买入两份', '买入一份', '静观其变',
                 '卖出一份', '卖出两份', '卖出三份', '卖出四份', '卖出六份', '卖出八份']]

    data = data[data['价格'] < data['二十倍市盈率']]

    data.to_excel(os.path.join(out_folder, '%s操作策略.xlsx' % (today)))

    data = data[data['价格'] < data['静观其变'] * 1.1]

    data.to_excel(os.path.join(out_folder, '%s可买入股票.xlsx' % (today)))

def filter_stock_by_cwbb(year):
    gplb = s_sum.get_summary_report_data()

    # 获取当前股票价格
    price_path = os.path.join(out_folder, '股票价格%s.csv' % (today))
    if not os.path.exists(price_path):
        ts.get_today_all().set_index('code').to_csv(price_path, encoding="utf-8")

    current_price = pd.read_csv(price_path, encoding="utf-8", index_col=0)
    current_price = current_price[['trade']]
    current_price.columns = ['价格']

    gplb = pd.merge(gplb, current_price, left_index=True, right_index=True)

    # 因为这里的平均利润单位是万元，而总股本单位是亿，价格单位是元
    gplb['平均市盈率'] = gplb['总股本'] * gplb['价格'] * 10000 / gplb['平均利润(万元)']
    gplb['平均股息率'] = gplb['总股本'] * gplb['价格'] * 10000 / gplb['平均股息(万元)']
    gplb = gplb[gplb['平均市盈率'] > 0]
    gplb = gplb[gplb['平均股息率'] > 0]

    gplb['平均利润率'] = gplb['平均利润(万元)'] / (gplb['营业总成本(万元)' + str(year)] + gplb['营业总成本(万元)' + str(year - 1)] + gplb['营业总成本(万元)' + str(year - 2)] +gplb['营业总成本(万元)' + str(year - 3)]) * 4 * 100

    #pianyi_func(gplb, year)

    for i in range(year - 2, year + 1):
        #gplb = gplb[gplb['利润总额(万元)' + str(i)] / gplb['生产资产(万元)' + str(i)] > 0.1]
        #gplb = gplb[gplb['非主业资产(万元)' + str(i)] / gplb['资产总计(万元)' + str(i)] < 0.2]
        #gplb = gplb[gplb['应收款(万元)' + str(i)] / gplb['资产总计(万元)' + str(i)] < 0.3]
        #gplb = gplb[gplb['有息负债(万元)' + str(i)] / gplb['资产总计(万元)' + str(i)] < 0.6]
        #gplb = gplb[gplb['其他应收款(万元)' + str(i)] / gplb['平均利润(万元)'] < 0.3]
        #gplb = gplb[gplb['其他应付款(万元)' + str(i)] / gplb['平均利润(万元)'] < 0.4]
        #gplb = gplb[abs(gplb['资产减值损失(万元)' + str(i)]) / gplb['平均利润(万元)'] < 0.3]

        #gplb = gplb[gplb['货币资金(万元)' + str(i)] / gplb['有息负债(万元)' + str(i)] > 1]
        #gplb = gplb[abs(gplb['营业外收入(万元)' + str(i)]) / abs(gplb['营业总收入(万元)' + str(i)]) < 0.4]
        #gplb = gplb[abs(gplb['营业外支出(万元)' + str(i)]) / abs(gplb['营业总成本(万元)' + str(i)]) < 0.4]

        #gplb = gplb[gplb['费用总和(万元)' + str(i)] / (gplb['营业总收入(万元)' + str(i)] - gplb['营业总成本(万元)' + str(i)]) < 1.1]
        gplb = gplb[gplb['经营活动产生的现金流量净额(万元)' + str(i)] > 5000]
        gplb = gplb[gplb['净利润(万元)' + str(i)] > 5000]

        #gplb = gplb[gplb['净利润增长率(%)'+ str(i)] > 0]
        #gplb = gplb[gplb['经营活动产生的现金流量净额(万元)' + str(i)] / abs(gplb['投资活动产生的现金流量净额(万元)' + str(i)]) > 0.4]

    gplb = gplb[gplb['现金及现金等价物的净增加额(万元)' + str(year)] + gplb['现金及现金等价物的净增加额(万元)' + str(year - 1)]
                + gplb['现金及现金等价物的净增加额(万元)' + str(year - 2)] + gplb['现金及现金等价物的净增加额(万元)' + str(year - 3)]> 0]

    gplb = gplb[gplb['应收款(万元)' + str(year)] / gplb['资产总计(万元)' + str(year)] < 0.2]
    gplb = gplb[gplb['货币资金(万元)' + str(year)] + (gplb['流动资产合计(万元)' + str(year)] - gplb['货币资金(万元)' + str(year)]) / 1.5 > gplb['负债合计(万元)' + str(year)] * 0.9]

    gplb = gplb[gplb['净利润增长率(%)'+ str(year)] + gplb['净利润增长率(%)'+ str(year - 1)] + gplb['净利润增长率(%)'+ str(year - 2)] > 20]
    gplb = gplb[(gplb['经营活动产生的现金流量净额(万元)' + str(year)] + gplb['经营活动产生的现金流量净额(万元)' + str(year - 1)] + gplb['经营活动产生的现金流量净额(万元)' + str(year - 2)] + gplb['经营活动产生的现金流量净额(万元)' + str(year - 3)]) / (gplb['净利润(万元)' + str(year)] + gplb['净利润(万元)' + str(year - 1)] + gplb['净利润(万元)' + str(year - 2)] + gplb['净利润(万元)' + str(year - 3)]) > 1]
    gplb = gplb[gplb['毛利率(%)' + str(year)] + gplb['毛利率(%)' + str(year - 1)] + gplb['毛利率(%)' + str(year - 2)] + gplb['毛利率(%)' + str(year - 3)] > 50]
    gplb = gplb[gplb['平均利润率'] > 22]


    file = os.path.join(out_folder, '%s%s财务报表评分后的公司%s.csv' % (calcu_end_year, month_day, today))
    gplb.to_csv(file, encoding='utf-8')

    operation_func(gplb, year)


def filter_stock_by_average_pe(src_path, min, max):
    data = pd.read_csv(src_path, index_col=0, encoding='utf-8')

    data = data[
        ['名字', '价格', '行业', '地区', '总股本', '总资产(万)', '市净率', '每股平均利润', '平均市盈率', '平均股息率', '平均利润率', '利润同比(%)', '毛利率(%)', '净利润率(%)', '平均利润(万元)', '净利润增长率(%)'+ str(calcu_end_year - 2), '净利润增长率(%)'+ str(calcu_end_year - 1), '净利润增长率(%)'+ str(calcu_end_year)]]

    print('\n%s:' % today)
    print()
    print('%d个公司' % data.shape[0])
    print('3年市盈率中位数%.1f' % round(data['平均市盈率'].median(), 1))
    print('市净率中位数%.1f' % round(data['市净率'].median(), 1))
    data = data[data['平均市盈率'] < (month_day == '-06-30' and max * 2 or max)]
    data = data[data['平均市盈率'] > min]
    data['平均市盈率'] = data['平均市盈率'].round(1)
    data['平均利润(万元)'] = data['平均利润(万元)'].round(1)
    data['市净率'] = data['市净率'].round(1)

    for i in range(calcu_end_year - 2, calcu_end_year + 1):
        data['净利润增长率(%)' + str(i)] = data['净利润增长率(%)' + str(i)].round(1)

    average_pe_file = \
        os.path.join(out_folder, '%s%s-3年平均市盈率在%s和%s之间的公司评分%s.xlsx' % (calcu_end_year, month_day, min, max, today))
    data.to_excel(average_pe_file)


if __name__ == '__main__':
    input_year = int(input('输入需要分析的年份：\n'))
    if input_year <= datetime.now().year:
        calcu_end_year = input_year

    choose = input('1: 年报 2: 半年报 3: 三季度报\n')

    if choose == '2':
        month_day = '-06-30'
    elif choose == '3':

        month_day = '-09-30'

    max_pe = int(input('输入最大市盈率：\n'))

    s_sum = stock_sum(calcu_end_year, month_day, stock_report_dir, out_folder)

    filter_stock_by_cwbb(calcu_end_year)

    filter_stock_by_average_pe(
        os.path.join(out_folder, '%s%s财务报表评分后的公司%s.csv' % (calcu_end_year, month_day, today))
        , 1, max_pe)  # 这个函数是根据平均pe过滤股票
