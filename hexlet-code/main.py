#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests as req
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
import numpy as np
import os
from dotenv import load_dotenv


# In[2]:


load_dotenv()

API_URL = os.getenv('API_URL')
DATE_BEGIN = os.getenv('DATE_BEGIN')
DATE_END = os.getenv('DATE_END')


# In[3]:


# API_URL='https://data-charts-api.hexlet.app'
# DATE_BEGIN = '2023-03-01'
# DATE_END = '2023-05-01'

def get_visits(API_URL, DATE_BEGIN, DATE_END):
    r_visits = req.get(f'{API_URL}/visits?begin={DATE_BEGIN}&end={DATE_END}')
    json_visits = r_visits.json()
    api_visits = pd.DataFrame(json_visits)
    api_visits['datetime'] = pd.to_datetime(api_visits['datetime'])
    visits = api_visits
    return visits


def get_registrations(API_URL, DATE_BEGIN, DATE_END):
    r_registrations = req.get(f'{API_URL}/registrations?begin={DATE_BEGIN}&end={DATE_END}')
    json_registrations = r_registrations.json()
    api_registrations = pd.DataFrame(json_registrations)
    api_registrations['datetime'] = pd.to_datetime(api_registrations['datetime'])
    registrations = api_registrations
    return registrations


def calc_and_save_conversion(visits, registrations): # до правок - "last_unique_visits(visits)"
    # удаляем строки с повторяющимися visit_id:
    unique_visits = visits.sort_values(by='datetime', ascending=False).drop_duplicates(subset = 'visit_id')
    # фильтруем строки от ботов:
    unique_visits = unique_visits[unique_visits['user_agent']!='bot']
    # приведение значений в столбце "datetime" к формату "YYYY-MM-DD":
    unique_visits['datetime'] = unique_visits['datetime'].dt.strftime('%Y-%m-%d')
    # группировка данных по полям "datetime", "platform" и сортировка по полю "datetime" от ранних дат к поздним:
    unique_visits = unique_visits.sort_values('datetime').groupby(['datetime', 'platform']).agg({'visit_id': 'count'}).reset_index()
    unique_visits = unique_visits.rename(columns={'datetime': 'date_group', 'visit_id': 'visits'})
    # приведение значений в столбце "datetime" к формату "YYYY-MM-DD":
    registrations['datetime'] = registrations['datetime'].dt.strftime('%Y-%m-%d')
    # группировка данных по полям "datetime", "platform" и сортировка по полю "datetime" от ранних дат к поздним:
    upd_registrations = registrations.sort_values('datetime').groupby(['datetime', 'platform']).agg({'user_id': 'count'}).reset_index()
    upd_registrations = upd_registrations.rename(columns={'datetime': 'date_group', 'user_id': 'registrations'})
    # объединение датафреймов visits, registrations:
    vis_regs = pd.merge(
        unique_visits, upd_registrations,
        left_on=['date_group', 'platform'],
        right_on=['date_group', 'platform'])
    # расчет конверсии:
    vis_regs['conversion'] = (vis_regs['registrations'] / vis_regs['visits'] * 100).round(2)
    # сохранение датафрейма с конверсией в формате JSON:
    vis_regs_conv = vis_regs.copy()
    vis_regs_conv.to_json('./conversion.json', orient='columns')
    return vis_regs_conv # датафрейм с конверсией


def get_ads(path):
    # чтение данных по рекламным кампаниям из файла *ads.csv*
    ads = pd.read_csv(f'{path}')
    ads['date'] = pd.to_datetime(ads['date'])
    # приведение значений в столбце "datetime" к формату "YYYY-MM-DD":
    ads['date'] = ads['date'].dt.strftime('%Y-%m-%d')
    ads = ads.rename(columns={'date': 'date_group'})
    return ads


def get_save_conversion_ads(visits, registrations):
    # объединение датафреймов по рекламе и конверсии
    visits = get_visits(API_URL, DATE_BEGIN, DATE_END)
    registrations = get_registrations(API_URL, DATE_BEGIN, DATE_END)
    df_conversion = calc_and_save_conversion(visits, registrations)
    ads = get_ads('./ads.csv')
    # объединение датафреймов по рекламе и конверсии
    df_conversion_ads = pd.merge(
        df_conversion, ads,
        left_on='date_group',
        right_on='date_group',
        how='inner')
    # удаление лишних столбцов "utm_source", "utm_medium":
    df_conversion_ads = df_conversion_ads.drop(['platform', 'conversion', 'utm_source', 'utm_medium'], axis=1)
    # замена пропусков по столбцам "cost", "utm_campaign" значениями "none" и "0" соответственно:
    df_conversion_ads = df_conversion_ads.fillna({'utm_campaign': 'none', 'cost':0})
    # группировка и сортировка данных от ранних дат к поздним по столбцу "date_group":
    df_conversion_ads = df_conversion_ads.sort_values('date_group').groupby(['date_group', 'utm_campaign']).sum().reset_index()
    # изменение порядка столбцов "cost", "utm_campaign" согласно требованиям:
    df_conversion_ads = df_conversion_ads[['date_group', 'visits', 'registrations', 'cost', 'utm_campaign']]
    # сохранение датафрейма с рекламными кампаниями в формате JSON:
    df_conversion_ads.to_json('./ads.json', orient='columns')
    return df_conversion_ads  # ранее 'conv_ads', 'upd_cnv_ads'


# Шаг 5. Визуализация расчетов
def charts(cnv_ads, df_conversion, registrations, ads, DATE_BEGIN, DATE_END):
    # график 'Total_visits_chart'
    fig, ax = plt.subplots(figsize=(15,10), tight_layout=True)
    bars = plt.bar(x='date_group', height='visits', zorder=2, data=cnv_ads)
    ax.bar_label(bars)
    plt.title('Total visits', fontsize=18)
    plt.xlabel('date_group')
    plt.ylabel('visits')
    ax.xaxis.set_major_locator(MultipleLocator(5))
    ax.tick_params(axis='both', which='major', length=7)
    plt.xticks(rotation=70)
    plt.grid(axis='y')
    plt.savefig('./charts/Total_visits_chart.png')
    plt.close(fig)

    
    # график 'Total_visits_by_platform_chart'
    fig, ax = plt.subplots(figsize=(15,10), tight_layout=True)
    df_conversion_pvt = df_conversion.pivot_table(index='date_group', columns='platform', values='visits')
    df_conversion_pvt.plot(kind='bar', stacked=True, ax=ax, zorder=2)
    plt.title('Visits by Platform (Stacked)', fontsize=24)
    plt.xlabel('Date_group', fontsize=19)
    plt.ylabel('Visits', fontsize=19)
    ax.tick_params(axis='both', labelsize=16)
    ax.xaxis.set_major_locator(MultipleLocator(5))
    ax.tick_params(which='major', length=7)
    plt.xticks(rotation=70)
    plt.grid(axis='y')
    plt.savefig('./charts/Total_visits_by_platform_chart.png')
    plt.close(fig)

    
    # график 'Total_registrations_chart'
    fig, ax = plt.subplots(figsize=(15,10), tight_layout=True)
    bars = plt.bar(x='date_group', height='registrations', zorder=2, data=cnv_ads)
    ax.bar_label(bars)
    plt.title('Total registrations', fontsize=16)
    plt.xlabel('date_group')
    plt.ylabel('registrations')
    ax.xaxis.set_major_locator(MultipleLocator(5))
    ax.tick_params(axis='both', which='major', length=7)
    plt.xticks(rotation=70)
    plt.grid(axis='y')
    plt.savefig('./charts/Total_registrations_chart.png')
    plt.close(fig)


    # график 'Total_registrations_by_platform_chart'
    fig, ax = plt.subplots(figsize=(15,10), tight_layout=True)
    df_conversion_pvt1 = df_conversion.pivot_table(index='date_group', columns='platform', values='registrations')
    df_conversion_pvt1.plot(kind='bar', stacked=True, ax=ax, zorder=2)
    plt.title('Total Registrations by Platform (Stacked)', fontsize=24)
    plt.legend(fontsize=16)
    plt.xlabel('Date_group', fontsize=19)
    plt.ylabel('Registrations', fontsize=19)
    ax.xaxis.set_major_locator(MultipleLocator(5))
    ax.tick_params(axis='both', labelsize=15, which='major', length=7)
    plt.xticks(rotation=70)
    plt.grid(axis='y')
    plt.savefig('./charts/Total_registrations_by_platform_chart.png')
    plt.close()

    
    # график 'Total_registr_by_type_chart'
    registrations['datetime'] = registrations['datetime'].astype('datetime64[ns]')
    # приведение значений в столбце "datetime" к формату "YYYY-MM-DD":
    registrations['datetime'] = registrations['datetime'].dt.strftime('%Y-%m-%d')
    # группировка данных по полям "datetime", "platform" и сортировка по полю "datetime" от ранних дат к поздним:
    upd_regs = registrations.sort_values('datetime').groupby(['datetime', 'registration_type']).agg({'user_id': 'count'}).reset_index()
    upd_regs = upd_regs.rename(columns={'datetime': 'date_group', 'user_id': 'registrations'})
    
    fig, ax = plt.subplots(figsize=(15,10), tight_layout=True)
    upd_regs_pvt = upd_regs.pivot_table(index='date_group', columns='registration_type', values='registrations')
    upd_regs_pvt.plot(kind='bar', stacked=True, ax=ax, zorder=2)
    plt.title('Total Registrations by Registration Type (Stacked)', fontsize=24)
    plt.legend(fontsize=16)
    plt.xlabel('Date_group', fontsize=19)
    plt.ylabel('Registrations', fontsize=19)
    ax.xaxis.set_major_locator(MultipleLocator(5))
    ax.tick_params(axis='both', labelsize=15, which='major', length=7)
    plt.xticks(rotation=70)
    plt.grid(axis='y')
    plt.savefig('./charts/Total_registr_by_type_chart.png')
    plt.close()


    # график 'Total_registr_by_platform_piechart'
    fig, axes = plt.subplots(1,2, figsize=(15,10), tight_layout=True)
    df_conversion.groupby(['platform']).sum().plot(ax=axes[0], kind='pie', y='registrations', autopct='%1.1f%%', fontsize=16, wedgeprops={"linewidth": 1, "edgecolor": "white"}, legend=False)
    upd_regs.groupby(['registration_type']).sum().plot(ax=axes[1], kind='pie', y='registrations', autopct='%1.1f%%', fontsize=16, wedgeprops={"linewidth": 1, "edgecolor": "white"}, legend=False)
    axes[0].set_title('Registrations by Platform', fontsize=20)
    axes[1].set_title('Registrations by Type', fontsize=20)
    plt.savefig('./charts/Total_registr_by_platform_piechart.png')
    plt.close(fig)


    # график 'Overall_conversion_chart'
    overall_conversion = cnv_ads.copy()
    overall_conversion['overall_conversion'] = (overall_conversion['registrations'] / overall_conversion['visits'] * 100).round(1)
    
    fig, ax = plt.subplots()
    fig.set_size_inches(15,10)
    fig.suptitle('Overall Conversion', fontsize=19)
    x = overall_conversion['date_group']
    y = overall_conversion['overall_conversion']
    ax.plot(x, y,
            marker="o",
            c="b",
            label='Общая конверсия',
            linewidth=2,
            markersize=6)
    for x,y in zip(x,y):
        label = "{:.0f}%".format(y)
        plt.annotate(label,
                     (x,y),
                     textcoords="offset points",
                     xytext=(0,10),
                     ha='center')
    mean_conversion = overall_conversion['overall_conversion'].mean()
    ax.axhline(y=mean_conversion, color='gray', linestyle='dashed', label='Average Conversion')
    ax.legend()
    ax.set_xlabel('Date', fontsize=14)
    ax.set_ylabel('Conversion (%)', fontsize=14)
    ax.xaxis.set_major_locator(MultipleLocator(5))
    ax.tick_params(axis='both', labelsize=9, which='major', length=7)
    plt.xticks(rotation=45)
    ax.grid(axis='y')
    plt.savefig('./charts/Overall_conversion_chart.png')
    plt.close(fig)


    # график 'Conversion_by_platform_chart'
    df_conversion_pvt = df_conversion.pivot_table(index='date_group', columns='platform', values='conversion')
    df_conversion_pvt.reset_index(inplace=True)
    fig, axes = plt.subplots(3,1, figsize=(20,22))

    x = df_conversion_pvt['date_group']
    y = df_conversion_pvt['android']
    axes[0].plot(x, y,
            marker="o",
            c="c",
            label='android',
            linewidth=2,
            markersize=10,
            zorder=2)
    axes[0].legend(fontsize=16)
    axes[0].set_title('Conversion android', fontsize=16)
    axes[0].set_xlabel('Date', fontsize=14)
    axes[0].set_ylabel('Conversion (%)', fontsize=14)
    axes[0].set_xticks(df_conversion_pvt['date_group'])
    axes[0].set_xticklabels(df_conversion_pvt['date_group'], fontsize=14, rotation=45)
    axes[0].grid(axis='y')
    
    x1 = df_conversion_pvt['date_group']
    y1 = df_conversion_pvt['ios']
    axes[1].plot(x1, y1,
            marker="o",
            c="c",
            label='ios',
            linewidth=2,
            markersize=10,
            zorder=2)
    axes[1].legend(fontsize=16)
    axes[1].set_title('Conversion ios', fontsize=16)
    axes[1].set_xlabel('Date', fontsize=14)
    axes[1].set_ylabel('Conversion (%)', fontsize=14)
    axes[1].set_xticks(df_conversion_pvt['date_group'])
    axes[1].set_xticklabels(df_conversion_pvt['date_group'], fontsize=14, rotation=45)
    axes[1].grid(axis='y')

    x2 = df_conversion_pvt['date_group']
    y2 = df_conversion_pvt['web']
    axes[2].plot(x2, y2,
            marker="o",
            c="c",
            label='web',
            linewidth=2,
            markersize=10,
            zorder=2)
    axes[2].legend(fontsize=16)
    axes[2].set_title('Conversion web', fontsize=16)
    axes[2].set_xlabel('Date', fontsize=14)
    axes[2].set_ylabel('Conversion (%)', fontsize=14)
    axes[2].set_xticks(df_conversion_pvt['date_group'])
    axes[2].set_xticklabels(df_conversion_pvt['date_group'], fontsize=14, rotation=45)
    axes[2].grid(axis='y')
    plt.tight_layout()
    plt.savefig('./charts/Conversion_by_platform_chart.png')
    plt.close(fig)


    # график 'Avg_conversion_chart'
    avg_conversion = df_conversion.groupby('date_group')['conversion'].mean().reset_index()
    fig, ax = plt.subplots()
    fig.set_size_inches(12,10)
    x = avg_conversion['date_group']
    y = avg_conversion['conversion']
    ax.plot(x, y,
            marker="o",
            c="g",
            label='Средняя конверсия',
            linewidth=1.5,
            markersize=6)
    ax.legend()
    ax.set_xlabel('Date')
    ax.set_ylabel('Conversion (%)')
    ax.grid(axis='y')
    plt.title('Average Conversion', fontsize=16)
    plt.xticks(avg_conversion['date_group'], rotation=65)
    plt.savefig('./charts/Avg_conversion_chart.png')
    plt.close(fig)


    # график 'Adcosts_chart'
    adcosts = ads[(ads['date_group'] > DATE_BEGIN) & (ads['date_group'] < DATE_END)].groupby('date_group')['cost'].mean().reset_index()
    # adcosts = ads.groupby('date_group')['cost'].mean().reset_index()
    fig, ax = plt.subplots()
    fig.set_size_inches(15,10)
    x = adcosts['date_group']
    y = adcosts['cost']
    ax.plot(x, y,
            marker="o",
            c="g",
            label='Средняя цена',
            linewidth=1.5,
            markersize=6)
    plt.title('Aggregated Ad Campaign Costs (by day)')
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Cost (RUB)', fontsize=12)
    plt.xticks(adcosts['date_group'], fontsize=10, rotation=45)
    ax.grid(axis='y')
    plt.savefig('./charts/Adcosts_chart.png')
    plt.close(fig)


    # график'Visits_and_registrations_chart':
    # отбор дат рекламных кампаний
    campaign_days = cnv_ads[['date_group', 'utm_campaign']]
    
    virtual_reality_workshop = campaign_days.loc[campaign_days['utm_campaign'] == 'virtual_reality_workshop']
    start_date = virtual_reality_workshop['date_group'].min()
    end_date = virtual_reality_workshop['date_group'].max()
    
    game_dev_crash_course = campaign_days.loc[campaign_days['utm_campaign'] == 'game_dev_crash_course']
    start_date = game_dev_crash_course['date_group'].min()
    end_date = game_dev_crash_course['date_group'].max()
    
    web_dev_workshop_series = campaign_days.loc[campaign_days['utm_campaign'] == 'web_dev_workshop_series']
    start_date = web_dev_workshop_series['date_group'].min()
    end_date = web_dev_workshop_series['date_group'].max()
    
    tech_career_fair = campaign_days.loc[campaign_days['utm_campaign'] == 'tech_career_fair']
    start_date = tech_career_fair['date_group'].min()
    end_date = tech_career_fair['date_group'].max()
    
    cybersecurity_special = campaign_days.loc[campaign_days['utm_campaign'] == 'cybersecurity_special']
    start_date = cybersecurity_special['date_group'].min()
    end_date = cybersecurity_special['date_group'].max()
    
    # создание датафрейма с периодами проведения рекламных кампаний
    periods_of_ads = pd.DataFrame(columns=['start_date', 'end_date', 'utm_campaign'])
    periods_of_ads['start_date'] = [virtual_reality_workshop['date_group'].min(), game_dev_crash_course['date_group'].min(), web_dev_workshop_series['date_group'].min(), tech_career_fair['date_group'].min(), cybersecurity_special['date_group'].min()]
    periods_of_ads['end_date'] = [virtual_reality_workshop['date_group'].max(), game_dev_crash_course['date_group'].max(), web_dev_workshop_series['date_group'].max(), tech_career_fair['date_group'].max(), cybersecurity_special['date_group'].max()]
    periods_of_ads['utm_campaign'] = ['virtual_reality_workshop', 'game_dev_crash_course', 'web_dev_workshop_series', 'tech_career_fair', 'cybersecurity_special']
    
    ads_started = periods_of_ads['start_date'].to_list()
    ads_ended = periods_of_ads['end_date'].to_list()

    fig, axes = plt.subplots(2, 1, figsize=(22,20))
    visits_campaign = cnv_ads.groupby(['date_group'])['visits'].sum().reset_index()
    x = visits_campaign['date_group']
    y = visits_campaign['visits']
    axes[0].plot(x, y,
            marker="o",
            c="c",
            label='Visits',
            linewidth=2,
            markersize=6)
    avg_visits = cnv_ads['visits'].mean()
    axes[0].axhline(y=avg_visits, color='gray', linestyle='dashed', label='Average Number of Visits')
    for i in range(len(ads_started)):
        axes[0].axvspan(ads_started[i], ads_ended[i], alpha=0.3, color=np.random.rand(3,), label=periods_of_ads['utm_campaign'].to_list()[i])
    axes[0].set_title('Visits during marketing active days', fontsize=22)
    axes[0].set_ylabel('Unique Visits', fontsize=20)
    axes[0].set_xticks(visits_campaign['date_group'])
    axes[0].set_xticklabels(visits_campaign['date_group'], fontsize=14, rotation=45)
    axes[0].legend(fontsize=14, loc='upper right')
    axes[0].grid(axis='y')
    registr_campaign = cnv_ads.groupby(['date_group'])['registrations'].sum().reset_index()
    x3 = registr_campaign['date_group']
    y3 = registr_campaign['registrations']
    axes[1].plot(x3, y3,
            marker="o",
            c="c",
            label='Registrations',
            linewidth=2,
            markersize=6)
    avg_registrations = cnv_ads['registrations'].mean()
    axes[1].axhline(y=avg_registrations, color='gray', linestyle='dashed', label='Average Number of Registrations')
    for i in range(len(ads_started)):
        axes[1].axvspan(ads_started[i], ads_ended[i], alpha=0.3, color=np.random.rand(3,), label=periods_of_ads['utm_campaign'].to_list()[i])
    axes[1].set_title('Registrations during marketing active days', fontsize=22)
    axes[1].set_ylabel('Unique Users', fontsize=20)
    axes[1].set_xticks(registr_campaign['date_group'])
    axes[1].set_xticklabels(registr_campaign['date_group'], fontsize=14, rotation=45)
    axes[1].legend(fontsize=14, loc='upper right')
    axes[1].grid(axis='y')
    plt.savefig('./charts/Visits_and_registrations_chart.png')
    plt.close(fig)


# In[4]:


def run_all():
    visits = get_visits(API_URL, DATE_BEGIN, DATE_END)
    registrations = get_registrations(API_URL, DATE_BEGIN, DATE_END)
    df_conversion = calc_and_save_conversion(visits, registrations)
    ads = get_ads('./ads.csv')
    cnv_ads = get_save_conversion_ads(visits, registrations)
    charts(cnv_ads, df_conversion, registrations, ads, DATE_BEGIN, DATE_END)


# In[5]:


if __name__ == "__main__":
    run_all()

