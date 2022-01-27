import pandas as pd
import numpy as np
data_by_years=[]
MIX_COLS = {
"Дата": "ДатаШихтовки",
"Состав шихты,%(спек.)": "% спек",
"Состав шихты,%(в т.ч. Марка Ж)": "% марка Ж",
"Состав шихты,%(в т.ч. кокс.)": "% кокс.",
"Оборот печей, час": "Оборот печей, ч",
"Качество шихты (помол,%)": "Помол, %",
"Качество шихты (пыль,%)": "Пыль, %",
"Качество шихты (Технический анализ, % — влага)": "Влага, %",
"Качество шихты (Технический анализ, % — зола)": "Зола, %",
"Качество шихты (Технический анализ, % — летуч.)": "Летуч., %",
"Качество шихты (Мд. Серы,%)": "Мд. Серы, %",
"Качество шихты (пласт. слой мм)": "Пласт. слой, мм",
}
features_keys=["ДатаШихтовки", "% спек", "% марка Ж", "% кокс.", "Оборот печей, ч", "Помол, %", "Пыль, %", "Влага, %", "Зола, %", "Летуч., %",
                "Мд. Серы, %", "Пласт. слой, мм"]

#concat all year data
for file in ["mixes_2017_b1.xlsx", "mixes_2018_b1.xlsx", "mixes_2019_b1.xlsx", "mixes_2020_b1.xlsx", "mixes_2021_b1.xlsx"]:
    data = pd.read_excel(file)
    data_by_years.append(data)
mix_data = pd.concat(data_by_years)
#fill NA in manufactors by zeroes
manuf_keys = [key for key in mix_data.keys() if key.find('Производитель')!=-1 ]
mix_data[manuf_keys] = mix_data[manuf_keys].fillna(value=0)

#merge manuf1 and manuf2
mix_data["Производитель 2"] = mix_data["Производитель 1 "] + mix_data["Производитель 2"]
mix_data.drop(columns=["Производитель 1 "], inplace=True)

#delete empty and special date
mix_data = mix_data[mix_data['Дата'].notna()]
df_data_del = pd.to_datetime(pd.DataFrame({'year': [2019, 2019, 2019],
                   'month': [8, 8, 11],
                   'day': [13, 14, 17]}))
for date in df_data_del:
    mix_data = mix_data[mix_data['Дата']!=date]

#create and save_target
target_data = mix_data[['Дата', 'Качество кокса,% (Показатели прочности,% — CSR)',
       'Качество кокса,% (Показатели прочности,% — M10)',
       'Качество кокса,% (Показатели прочности,% — M25)']]
target_data.rename(columns={'Дата':'ДатаПробыКокса', 'Качество кокса,% (Показатели прочности,% — CSR)': 'CSR',
                            'Качество кокса,% (Показатели прочности,% — M10)':'M10',
                            'Качество кокса,% (Показатели прочности,% — M25)':'M25'}, inplace=True)
target_data.to_excel("target_b1.xlsx")

#create features
#add properties
properties_data = pd.read_excel("properties.xlsx")
properties_data.drop_duplicates(inplace=True)
properties_keys = ['R0', 'σ', 'Vt', 'I', 'Io', 'SI', 'КТЦ эксп.',
                   'MF', 'MF_spec', 'MF_otosh', 'CSR_carb']

# manuf_with_no_properties_col = []
properties_dict_col = {prop_key: [] for prop_key in properties_keys}
manuf_keys = [key for key in mix_data.keys() if key.find('Производитель')!=-1 ]
#calulate properties for row
for i in range(len(mix_data)):
    prop_dict_for_sample = {prop_key: [] for prop_key in properties_keys}
    manuf_with_no_props = []
    weights = []
    for manuf in manuf_keys:
        #continue if this manufactor don't participate in this sample
        if np.isnan(mix_data.iloc[i, mix_data.columns.get_loc(manuf)]) or mix_data.iloc[i, mix_data.columns.get_loc(manuf)]==0:
            continue
        year = mix_data.iloc[i,mix_data.columns.get_loc("Дата")].year
        properties_for_manuf_in_year = properties_data[(properties_data["Год"] == year) & (properties_data["Концентрат"] == manuf)]
        #if manufactor in this year don't have properties append it to manuf_with_no_props
        if len(properties_for_manuf_in_year)==0:
            manuf_with_no_props.append(manuf + ":" + str(mix_data.iloc[i, mix_data.columns.get_loc(manuf)]))

        else:
            weights.append(mix_data.iloc[i, mix_data.columns.get_loc(manuf)]/100)
            for prop in properties_keys:
                prop_dict_for_sample[prop].append(float(properties_for_manuf_in_year.iloc[0, properties_for_manuf_in_year.columns.get_loc(prop)]))
    for prop in properties_keys:
        assert len(weights)==len(prop_dict_for_sample[prop])
        if len(weights) == len(prop_dict_for_sample[prop])==0:
            properties_dict_col[prop].append(0)
        else:
            properties_dict_col[prop].append(np.average(prop_dict_for_sample[prop], weights=weights))
        # add manufactor who hasn't properties to list
    # manuf_with_no_properties_col.append("|".join(manuf_with_no_props))
#add columns with properties to mix_data
for key in properties_keys:
    mix_data[key] = properties_dict_col[key]
# mix_data["manuf_with_no_properties_col"] = manuf_with_no_properties_col
mix_data.rename(columns=MIX_COLS, inplace=True)
[features_keys.append(key) for key in properties_keys]
features_data = mix_data[features_keys]
features_data["Оборот печей отсеч, ч"] = features_data["Оборот печей, ч"].apply(lambda x: x if x>=18 else 18)
features_data.to_excel("features_b1.xlsx")
# keys_order.append("manuf_with_no_properties_col")
#merge targets and features
aligned_data = pd.merge(features_data, target_data, left_on=["ДатаШихтовки"], right_on=["ДатаПробыКокса"])
aligned_data.to_excel("aligned_b1.xlsx")