#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd
import requests as req
import matplotlib.pyplot as plt
import os


# In[ ]:


from dotenv import load_dotenv

load_dotenv()  # загружаются переменные из файла

# SOME_VAR = os.getenv('SOME_VAR')
API_URL = os.getenv('API_URL')
DATE_BEGIN = os.getenv('DATE_BEGIN')
DATE_END = os.getenv('DATE_END')


# In[2]:


def run_all():
    # API_URL=https://data-charts-api.hexlet.app
    # date_begin = '2023-03-01'
    # date_end = '2023-09-01'
    def get_visits(API_URL, DATE_BEGIN, DATE_END):
        r_visits = req.get(f'{API_URL}/visits?begin={DATE_BEGIN}&end={DATE_END}')
        json_visits = r_visits.json()
        api_visits = pd.DataFrame(json_visits)
        return api_visits
    visits = get_visits(API_URL, DATE_BEGIN, DATE_END)


    # date_begin = '2023-03-01'
    # date_end = '2023-09-01'
    def get_registrations(API_URL, DATE_BEGIN, DATE_END):
        r_registrations = req.get(f'{API_URL}/registrations?begin={DATE_BEGIN}&end={DATE_END}')
        json_registrations = r_registrations.json()
        api_registrations = pd.DataFrame(json_registrations)
        return api_registrations
    registrations = get_registrations(API_URL, DATE_BEGIN, DATE_END)
    
    
    def visits_without_bots(visits):
        filtered_visits = visits[visits['user_agent'].str.contains("bot")==False]
        return filtered_visits
    clean_visits = visits_without_bots(visits)


    def last_unique_visits(clean_visits):
        # удаляем строки с повторяющимися visit_id:
        unique_visits = clean_visits.sort_values(by='datetime', ascending=False).drop_duplicates(subset = 'visit_id')
        # приведение значений в столбце "datetime" к смешанному формату:
        unique_visits['datetime'] = pd.to_datetime(unique_visits['datetime'], format='mixed')
        # приведение значений в столбце "datetime" к формату "YYYY-MM-DD":
        unique_visits['datetime'] = unique_visits['datetime'].dt.strftime('%Y-%m-%d')
        # группировка данных по полям "datetime", "platform" и сортировка по полю "datetime" от ранних дат к поздним:
        unique_vis = unique_visits.sort_values('datetime').groupby(['datetime', 'platform']).agg({'visit_id': 'count'}).reset_index()
        unique_vis = unique_vis.rename(columns={'datetime': 'date_group', 'visit_id': 'visits'})
        return unique_vis    
    result_vis = last_unique_visits(clean_visits)


    def update_regs(registrations):
        # приведение значений в столбце "datetime" к смешанному формату:
        registrations['datetime'] = pd.to_datetime(registrations['datetime'], format='mixed')
        # приведение значений в столбце "datetime" к формату "YYYY-MM-DD":
        registrations['datetime'] = registrations['datetime'].dt.strftime('%Y-%m-%d')
        # группировка данных по полям "datetime", "platform" и сортировка по полю "datetime" от ранних дат к поздним:
        regs = registrations.sort_values('datetime').groupby(['datetime', 'platform']).agg({'user_id': 'count'}).reset_index()
        regs = regs.rename(columns={'datetime': 'date_group', 'user_id': 'registrations'})
        return regs
    result_regs = update_regs(registrations)


    def merged_visits_and_registrations(result_vis, result_regs):
        vis_regs = pd.merge(
            result_vis, result_regs,
            left_on=['date_group', 'platform'],
            right_on=['date_group', 'platform'])
        return vis_regs    
    vis_regs = merged_visits_and_registrations(result_vis, result_regs)


    def conversion(vis_regs):
        vis_regs['conversion'] = (vis_regs['registrations'] / vis_regs['visits'] * 100).round(2)
        return vis_regs
    vis_reg_conversion = conversion(vis_regs)


    def save_conversion_to_json(vis_reg_conversion):
        vis_reg_conversion_json = vis_reg_conversion.to_json('./conversion.json', orient='columns')
    save_conversion_to_json(vis_reg_conversion)


    def get_ads(orders_path):
        ads = pd.read_csv(f'{orders_path}')
        return ads
    ads = get_ads('./ads.csv')


    def update_ads(ads):
        # приведение значений в столбце "datetime" к смешанному формату:
        ads['date'] = pd.to_datetime(ads['date'], format='mixed')
        # приведение значений в столбце "datetime" к формату "YYYY-MM-DD":
        ads['date'] = ads['date'].dt.strftime('%Y-%m-%d')
        ads = ads.rename(columns={'date': 'date_group'})
        return ads
    upd_ads = update_ads(ads)


    def merged_conversion_and_ads(vis_reg_conversion, upd_ads):
        conv_ads = pd.merge(
            vis_reg_conversion, upd_ads,
            left_on='date_group',
            right_on='date_group',
            how='inner')
        return conv_ads
    cnv_ads = merged_conversion_and_ads(vis_reg_conversion, upd_ads)


    def update_cnv_ads(cnv_ads):
        # удаление лишних столбцов "utm_source", "utm_medium":
        cnv_ads = cnv_ads.drop(['platform', 'conversion', 'utm_source', 'utm_medium'], axis=1)
        # замена пропусков по столбцам "cost", "utm_campaign" значениями "none" и "0" соответственно:
        cnv_ads = cnv_ads.fillna({'utm_campaign': 'none', 'cost':0})
        # группировка и сортировка данных от ранних дат к поздним по столбцу "date_group":
        cnv_ads = cnv_ads.sort_values('date_group').groupby(['date_group', 'utm_campaign']).sum().reset_index()
        # изменение порядка столбцов "cost", "utm_campaign" согласно требованиям:
        cnv_ads = cnv_ads[['date_group', 'visits', 'registrations', 'cost', 'utm_campaign']]
        return cnv_ads
    upd_cnv_ads = update_cnv_ads(cnv_ads)


    def save_cnv_ads_to_json(upd_cnv_ads):
        upd_cnv_ads = upd_cnv_ads.to_json('./ads.json', orient='columns')
    save_cnv_ads_to_json(upd_cnv_ads)


    def total_visits_chart(upd_cnv_ads):
        fig, ax = plt.subplots(figsize=(22,20))
        bars = plt.bar(x='date_group', height='visits', data=upd_cnv_ads)
        ax.bar_label(bars)
        plt.title('Total visits')
        plt.xlabel('date_group')
        plt.ylabel('visits')
        plt.xticks(upd_cnv_ads['date_group'][::9], rotation=45)
        plt.grid(axis='y')
        plt.tight_layout()
        plt.savefig('./charts/Total_visits_chart.png')
        plt.close(fig)
    total_visits_chart(upd_cnv_ads)


    def total_visits_pltfm_chart(cnv_ads):
        fig, ax = plt.subplots(figsize=(22,10))
        cnv_ads_pvt = cnv_ads.pivot_table(index='date_group', columns='platform', values='visits')
        cnv_ads_pvt.plot(kind='bar', stacked=True, ax=ax)
        plt.title('Visits by Platform (Stacked)', fontsize=16)
        plt.xlabel('date_group')
        plt.ylabel('visits')
        plt.xticks(rotation=45)
        plt.grid(axis='y')
        plt.tight_layout()
        plt.savefig('./charts/Total_visits_by_platform_chart.png')
        plt.close()
    total_visits_pltfm_chart(cnv_ads)


    def total_registrations_chart(upd_cnv_ads):
        fig, ax = plt.subplots(figsize=(22,20))
        bars = plt.bar(x='date_group', height='registrations', data=upd_cnv_ads)
        ax.bar_label(bars)
        plt.title('Total registrations')
        plt.xlabel('date_group')
        plt.ylabel('registrations')
        plt.xticks(upd_cnv_ads['date_group'][::9], rotation=45)
        plt.grid(axis='y')
        plt.tight_layout()
        plt.savefig('./charts/Total_registrations_chart.png')
        plt.close(fig)
    total_registrations_chart(upd_cnv_ads)


    def total_registrations_pltfm_chart(cnv_ads):
        fig, ax = plt.subplots(figsize=(22,10))
        cnv_ads_pvt1 = cnv_ads.pivot_table(index='date_group', columns='platform', values='registrations')
        cnv_ads_pvt1.plot(kind='bar', stacked=True, ax=ax)
        plt.title('Total Registrations by Platform (Stacked)', fontsize=16)
        plt.xlabel('date_group')
        plt.ylabel('registrations')
        plt.xticks(rotation=45)
        plt.grid(axis='y')
        plt.tight_layout()
        plt.savefig('./charts/Total_registrations_by_platform_chart.png')
        plt.close()
    total_registrations_pltfm_chart(cnv_ads)


    def upd_registr(registrations):
        # приведение значений в столбце "datetime" к смешанному формату:
        registrations['datetime'] = pd.to_datetime(registrations['datetime'], format='mixed')
        # приведение значений в столбце "datetime" к формату "YYYY-MM-DD":
        registrations['datetime'] = registrations['datetime'].dt.strftime('%Y-%m-%d')
        # группировка данных по полям "datetime", "platform" и сортировка по полю "datetime" от ранних дат к поздним:
        upd_regs = registrations.sort_values('datetime').groupby(['datetime', 'registration_type']).agg({'user_id': 'count'}).reset_index()
        upd_regs = upd_regs.rename(columns={'datetime': 'date_group', 'user_id': 'registrations'})
        return upd_regs
    upd_regs1 = upd_registr(registrations)


    def total_registr_type_chart(upd_regs1):
        fig, ax = plt.subplots(figsize=(22,10))
        upd_regs1_pvt = upd_regs1.pivot_table(index='date_group', columns='registration_type', values='registrations')
        upd_regs1_pvt.plot(kind='bar', stacked=True, ax=ax)
        plt.title('Total Registrations by Registration Type (Stacked)')
        plt.xlabel('date_group')
        plt.ylabel('registrations')
        plt.xticks(rotation=45)
        plt.grid(axis='y')
        plt.tight_layout()
        plt.savefig('./charts/Total_registr_by_type_chart.png')
        plt.close()
    total_registr_type_chart(upd_regs1)


    def total_registr_pltfm_types_piechart(cnv_ads, upd_regs1):
        fig, axes = plt.subplots(1, 2, figsize=(15,10))
        cnv_ads.groupby(['platform']).sum().plot(ax=axes[0], kind='pie', y='registrations', autopct='%1.1f%%', fontsize=16, wedgeprops={"linewidth": 1, "edgecolor": "white"}, legend=False)
        upd_regs1.groupby(['registration_type']).sum().plot(ax=axes[1], kind='pie', y='registrations', autopct='%1.1f%%', fontsize=16, wedgeprops={"linewidth": 1, "edgecolor": "white"}, legend=False)
        axes[0].set_title('Registrations by Platform', fontsize=20)
        axes[1].set_title('Registrations by Type', fontsize=20)
        plt.tight_layout()
        plt.savefig('./charts/Total_registr_by_platform_piechart.png')
        plt.close(fig)
    total_registr_pltfm_types_piechart(cnv_ads, upd_regs1)


    def overall_cnv(upd_cnv_ads):
        upd_cnv_ads['overall_conversion'] = (upd_cnv_ads['registrations'] / upd_cnv_ads['visits'] * 100).round(1)
        return upd_cnv_ads
    overall_cnv = overall_cnv(upd_cnv_ads)


    def overall_conversion_chart(overall_cnv):
        fig, ax = plt.subplots()
        fig.set_size_inches(20,10)
        fig.suptitle('Overall Conversion', fontsize=20)
        x = overall_cnv['date_group']
        y = overall_cnv['overall_conversion']
        ax.plot(x, y,
                marker="o",
                c="b",
                label='Общая конверсия',
                linewidth=2,
                markersize=10)
        for x,y in zip(x,y):
            label = "{:.0f}%".format(y)
            plt.annotate(label,
                         (x,y),
                         textcoords="offset points",
                         xytext=(0,10),
                         ha='center')
        ax.legend()
        ax.set_xlabel('Date')
        ax.set_ylabel('Conversion (%)')
        ax.grid(axis='y')
        plt.xticks(overall_cnv['date_group'][::9], rotation=45)
        plt.savefig('./charts/Overall_conversion_chart.png')
        plt.close(fig)
    overall_conversion_chart(overall_cnv)


    def conversion_pltfm_chart(cnv_ads):
        cnv_ads_pvt = cnv_ads.pivot_table(index='date_group', columns='platform', values='conversion')
        cnv_ads_pvt.reset_index(inplace=True)
        fig, axes = plt.subplots(3, 1, figsize=(22,20))
    
        x = cnv_ads_pvt['date_group']
        y = cnv_ads_pvt['android']
        axes[0].plot(x, y,
                marker="o",
                c="c",
                label='android',
                linewidth=2,
                markersize=10)
        axes[0].legend()
        axes[0].set_title('Conversion android')
        axes[0].set_xlabel('Date')
        axes[0].set_ylabel('Conversion (%)')
        axes[0].set_xticks(cnv_ads_pvt['date_group'])
        axes[0].set_xticklabels(cnv_ads_pvt['date_group'], rotation=45)
        axes[0].grid(axis='y')
    
        x1 = cnv_ads_pvt['date_group']
        y1 = cnv_ads_pvt['ios']
        axes[1].plot(x1, y1,
                marker="o",
                c="c",
                label='ios',
                linewidth=2,
                markersize=10)
        axes[1].legend()
        axes[1].set_title('Conversion ios')
        axes[1].set_xlabel('Date')
        axes[1].set_ylabel('Conversion (%)')
        axes[1].set_xticks(cnv_ads_pvt['date_group'])
        axes[1].set_xticklabels(cnv_ads_pvt['date_group'], rotation=45)
        axes[1].grid(axis='y')

        x2 = cnv_ads_pvt['date_group']
        y2 = cnv_ads_pvt['web']
        axes[2].plot(x2, y2,
                marker="o",
                c="c",
                label='web',
                linewidth=2,
                markersize=10)
        axes[2].legend()
        axes[2].set_title('Conversion web')
        axes[2].set_xlabel('Date')
        axes[2].set_ylabel('Conversion (%)')
        axes[2].set_xticks(cnv_ads_pvt['date_group'])
        axes[2].set_xticklabels(cnv_ads_pvt['date_group'], rotation=45)
        axes[2].grid(axis='y')
        plt.tight_layout()
        plt.savefig('./charts/Conversion_by_platform_chart.png')
        plt.close(fig)
    conversion_pltfm_chart(cnv_ads)

    
    def avg_conversion_chart(cnv_ads):
        avg_conversion = cnv_ads.groupby('date_group')['conversion'].mean().reset_index()
        fig, ax = plt.subplots()
        fig.set_size_inches(15,10)
        x = avg_conversion['date_group']
        y = avg_conversion['conversion']
        ax.plot(x, y,
                marker="o",
                c="g",
                label='Средняя конверсия',
                linewidth=1.5,
                markersize=6)
        '''for x,y in zip(x,y):
            label = "{:.0f}%".format(y)
            plt.annotate(label,
                         (x,y),
                         textcoords="offset points",
                         xytext=(0,10),
                         ha='center')'''
        ax.legend()
        ax.set_xlabel('Date')
        ax.set_ylabel('Conversion (%)')
        ax.grid(axis='y')
        plt.title('Average Conversion')
        plt.xticks(avg_conversion['date_group'][::9], rotation=45)
        plt.savefig('./charts/Avg_conversion_chart.png')
        plt.close(fig)
    avg_conversion_chart(cnv_ads)    

    
    def adcosts_chart(cnv_ads):
        adcosts = cnv_ads.groupby('date_group')['cost'].mean().reset_index()
        fig, ax = plt.subplots()
        fig.set_size_inches(15,10)
        x = adcosts['date_group']
        y = adcosts['cost']
        ax.plot(x, y,
                marker="o",
                c="g",
                label='Средняя конверсия',
                linewidth=1.5,
                markersize=6)
        '''for x,y in zip(x,y):
            label = "{:.0f}%".format(y)
            plt.annotate(label,
                         (x,y),
                         textcoords="offset points",
                         xytext=(0,10),
                         ha='center')'''
        ax.legend()
        ax.set_xlabel('Date')
        ax.set_ylabel('Cost (RUB)')
        ax.grid(axis='y')
        plt.title('Aggregated Ad Campaign Costs (by day)')
        plt.xticks(adcosts['date_group'][::9], rotation=45)
        plt.savefig('./charts/Adcosts_chart.png')
        plt.close(fig)
    adcosts_chart(cnv_ads)


    # отбор рекламных кампаний:
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

    # создание датафрейма с периодами проведения рекламных кампаний:
    periods_of_ads = pd.DataFrame(columns=['start_date', 'end_date', 'utm_campaign'])
    periods_of_ads['start_date'] = [virtual_reality_workshop['date_group'].min(), game_dev_crash_course['date_group'].min(), web_dev_workshop_series['date_group'].min(), tech_career_fair['date_group'].min(), cybersecurity_special['date_group'].min()]
    periods_of_ads['end_date'] = [virtual_reality_workshop['date_group'].max(), game_dev_crash_course['date_group'].max(), web_dev_workshop_series['date_group'].max(), tech_career_fair['date_group'].max(), cybersecurity_special['date_group'].max()]
    periods_of_ads['utm_campaign'] = ['virtual_reality_workshop', 'game_dev_crash_course', 'web_dev_workshop_series', 'tech_career_fair', 'cybersecurity_special']

    import numpy as np

    def vis_regist_campaign_chart(cnv_ads):
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
        ads_started = periods_of_ads['start_date'].to_list()
        ads_ended = periods_of_ads['end_date'].to_list()
        for i in range(len(ads_started)):
            axes[0].axvspan(ads_started[i], ads_ended[i], alpha=0.3, color=np.random.rand(3,), label=periods_of_ads['utm_campaign'].to_list()[i])
        axes[0].set_title('Visits during marketing active days')
        axes[0].set_ylabel('Unique Visits')
        axes[0].set_xticks(visits_campaign['date_group'][::9])
        axes[0].set_xticklabels(visits_campaign['date_group'][::9], fontsize=12, rotation=45)
        axes[0].legend()
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
        axes[1].axhline(y=avg_visits, color='gray', linestyle='dashed', label='Average Number of Registrations')
        for i in range(len(ads_started)):
            axes[1].axvspan(ads_started[i], ads_ended[i], alpha=0.3, color=np.random.rand(3,), label=periods_of_ads['utm_campaign'].to_list()[i])
        axes[1].set_title('Registrations during marketing active days')
        axes[1].set_ylabel('Unique Users')
        axes[1].set_xticks(registr_campaign['date_group'][::9])
        axes[1].set_xticklabels(registr_campaign['date_group'][::9], fontsize=12, rotation=45)
        axes[1].legend()
        axes[1].grid(axis='y')
        plt.savefig('./charts/Visits_and_registrations_chart.png')
        plt.close(fig)
    vis_regist_campaign_chart(cnv_ads)


# In[ ]:


run_all()

