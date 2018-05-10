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

def score_func(data, year):
    data['评分'] = 0
    for index, row in data.iterrows():
        scroe_sum = 0

        tiaojian = row['利润总额(万元)'] / row['生产资产(万元)'] 
        if tiaojian > 0.3:
            scroe_sum += 20
        elif tiaojian > 0.1:
            scroe_sum += 10
        else:
            scroe_sum -= 20

        tiaojian = row['非主业资产(万元)'] / row['总资产(万)']
        if tiaojian < 0.05:
            scroe_sum += 20
        elif tiaojian < 0.1:
            scroe_sum += 10
        else:
            scroe_sum -= 20
            
        tiaojian = row['应收款(万元)'] / row['总资产(万)']
        if tiaojian < 0.1:
            scroe_sum += 50
        elif tiaojian < 0.3:
            scroe_sum += 20
        else:
            scroe_sum -= 50
            
        tiaojian = row['有息负债(万元)'] / row['总资产(万)']
        if tiaojian < 0.4:
            scroe_sum += 20
        elif tiaojian < 0.6:
            scroe_sum += 10
        else:
            scroe_sum -= 20
        
        tiaojian = row['其他应收款(万元)'] / row['平均利润(万元)']
        if tiaojian < 0.05:
            scroe_sum += 50
        elif tiaojian < 0.2:
            scroe_sum += 20
        else:
            scroe_sum -= 50
            
        tiaojian = row['其他应付款(万元)'] / row['平均利润(万元)']
        if tiaojian < 0.1:
            scroe_sum += 20
        elif tiaojian < 0.3:
            scroe_sum += 10
        else:
            scroe_sum -= 20
            
        tiaojian = abs(row['资产减值损失(万元)'] / row['平均利润(万元)'])
        if tiaojian < 0.2:
            scroe_sum += 20
        elif tiaojian < 0.35:
            scroe_sum += 10
        else:
            scroe_sum -= 20
            
        tiaojian = row['货币资金(万元)'] / (row['有息负债(万元)'] + 0.00001)
        if tiaojian > 1:
            scroe_sum += 50
        elif tiaojian > 0.8:
            scroe_sum += 20
        else:
            scroe_sum -= 50
        
        tiaojian = row['利润同比(%)']
        if tiaojian > 20:
            scroe_sum += 50
        elif tiaojian > 0:
            scroe_sum += 20
        else:
            scroe_sum -= 50
            
        tiaojian = abs(row['营业外收入(万元)']) / abs(row['营业总收入(万元)'])
        if tiaojian < 0.1:
            scroe_sum += 20
        elif tiaojian < 0.2:
            scroe_sum += 10
        else:
            scroe_sum -= 20
        
        tiaojian = abs(row['营业外支出(万元)']) / abs(row['营业总成本(万元)'])
        if tiaojian < 0.1:
            scroe_sum += 20
        elif tiaojian < 0.2:
            scroe_sum += 10
        else:
            scroe_sum -= 20
            
        tiaojian = row['毛利率(%)']
        if tiaojian > 50:
            scroe_sum += 50
        elif tiaojian > 20:
            scroe_sum += 20
        else:
            scroe_sum -= 50
            
        tiaojian = row['费用总和(万元)'] / (row['营业总收入(万元)'] - row['营业总成本(万元)'] + 0.00001)
        if tiaojian < 0.6:
            scroe_sum += 50
        elif tiaojian < 0.8:
            scroe_sum += 20
        else:
            scroe_sum -= 50

        for i in range(year - 2, year + 1):
            tiaojian = row['经营活动产生的现金流量净额(万元)' + str(i)] / abs(row['投资活动产生的现金流量净额(万元)' + str(i)])
            if tiaojian > 2:
                scroe_sum += 10
            elif tiaojian > 0.5:
                scroe_sum += 50
            else:
                scroe_sum -= 10
            
            tiaojian = row['经营活动产生的现金流量净额(万元)' + str(i)] / row['净利润(万元)' + str(i)]
            if tiaojian > 2:
                scroe_sum += 30
            elif tiaojian > 1:
                scroe_sum += 10
            else:
                scroe_sum -= 30

            tiaojian = row['现金及现金等价物的净增加额(万元)' + str(i)]
            if tiaojian > 0:
                scroe_sum += 10
            else:
                scroe_sum -= 10

        data.loc[index, '评分'] = scroe_sum
    return data


def filter_stock_by_cwbb(year):
    gplb = s_sum.get_summary_report_data()

    gplb = gplb[gplb['利润总额(万元)'] / gplb['生产资产(万元)'] > 0.1]
    gplb = gplb[gplb['非主业资产(万元)'] / gplb['总资产(万)'] < 0.2]
    gplb = gplb[gplb['应收款(万元)'] / gplb['总资产(万)'] < 0.3]
    gplb = gplb[gplb['有息负债(万元)'] / gplb['总资产(万)'] < 0.6]
    gplb = gplb[gplb['其他应收款(万元)'] / gplb['平均利润(万元)'] < 0.3]
    gplb = gplb[gplb['其他应付款(万元)'] / gplb['平均利润(万元)'] < 0.4]
    gplb = gplb[abs(gplb['资产减值损失(万元)']) / gplb['平均利润(万元)'] < 0.3]

    gplb = gplb[gplb['货币资金(万元)'] / gplb['有息负债(万元)'] > 1]
    gplb = gplb[gplb['利润同比(%)'] > 0]
    gplb = gplb[abs(gplb['营业外收入(万元)']) / abs(gplb['营业总收入(万元)']) < 0.4]
    gplb = gplb[abs(gplb['营业外支出(万元)']) / abs(gplb['营业总成本(万元)']) < 0.4]
    gplb = gplb[gplb['毛利率(%)'] > 20]

    gplb = gplb[gplb['费用总和(万元)'] / (gplb['营业总收入(万元)'] - gplb['营业总成本(万元)']) < 1]

    for i in range(year - 2, year + 1):
        gplb = gplb[gplb['经营活动产生的现金流量净额(万元)' + str(i)] > 0]
        gplb = gplb[gplb['净利润(万元)' + str(i)] > 0]

    gplb = score_func(gplb, year)

    gplb = gplb[gplb['评分'] > 400]

    file = os.path.join(out_folder, '%s%s财务报表评分后的公司%s.csv' % (calcu_end_year, month_day, today))
    gplb.to_csv(file, encoding='utf-8')

def filter_stock_by_average_pe(src_path, min, max):
    gplb = pd.read_csv(src_path, index_col=0, encoding='utf-8')

    # 获取当前股票价格
    price_path = os.path.join(out_folder, '股票价格%s.csv' % (today))
    if not os.path.exists(price_path):
        ts.get_today_all().set_index('code').to_csv(price_path, encoding="utf-8")

    current_price = pd.read_csv(price_path, encoding="utf-8", index_col=0)
    current_price = current_price[['trade']]
    current_price.columns = ['价格']
    gplb = gplb[
        ['名字', '行业', '地区', '流通股本', '总股本', '总资产(万)', '市净率', '利润同比(%)', '毛利率(%)', '净利润率(%)', '平均利润(万元)', '评分']]

    data = pd.merge(gplb, current_price, left_index=True, right_index=True)
    # 因为这里的平均利润单位是万元，而总股本单位是亿，价格单位是元
    data['平均市盈率'] = data['总股本'] * data['价格'] * 10000 / data['平均利润(万元)']
    print('\n%s:' % today)
    print()
    print('%d个公司' % data.shape[0])
    print('3年市盈率中位数%.1f' % round(data['平均市盈率'].median(), 1))
    print('市净率中位数%.1f' % round(data['市净率'].median(), 1))
    data = data[data['平均市盈率'] < (month_day == '-06-30' and max * 2 or max)]
    data = data[data['平均市盈率'] > min]
    data['平均市盈率'] = data['平均市盈率'].round(1)
    data['平均利润(万元)'] = data['平均利润(万元)'].round()
    data['市净率'] = data['市净率'].round(1)
    data['总股本'] = data['总股本'].round()
    data['流通股本'] = data['流通股本'].round()
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
