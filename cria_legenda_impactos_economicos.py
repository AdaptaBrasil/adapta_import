# -*- coding: utf-8 -*-
import pandas as pd
#from src.db import dbThings

colors_zero = ['#01665E','#35978F','#80CDC1','#F5F5F5','#DFC27D','#BF812D','#8C510A']

colors_positive = ['#F6E8C3', '#DFC27D', '#BF812D', '#8C510A', '#543005']

colors_negative = ['#C7EAE5','#80CDC1','#35978F','#01665E','#003C30']

labels = ['N1','N2','N3','ZERO',
          'P1','P2','P3','P4','P5']

undefined = '#DCDCDC'
def execute():
    #conn = dbThings.get_connection()
    i = 1
    legends = pd.read_excel(r'D:\Atrium\Projects\AdaptaBrasil\Data\intervalos_legenda.xlsx')
    #legends = legends.query('id == 32').reset_index()
    csv = 'id,symbol,color,minvalue,maxvalue,legend_id,order,tag\n'
    for index, row_l in legends.iterrows():
        print('Processando: ',row_l['indicator_id'])

        limits = []
        _min, _max = round(float(legends.iloc[index]._min), 2), round(float(legends.iloc[index]._max), 2)

        positive_negative_0 = 'p' if _min >= 0 and _max >= 0 else ('n' if _min < 0 and _max <= 0 else 'z')
        _count = 7 if positive_negative_0 == 'z' else 5
        if positive_negative_0 == 'z':
            _min, _max = _min, float(-0.01)
            step = round((_max - _min) / 3, 2)
            for j in range(3):
                limits.append([round(j * step + _min, 2), round((j + 1) * step + _min - 0.01,2)])
            limits[len(limits)-1][1] = -0.01 # rounding probs
            limits.append([0, 0.01])

            _min, _max = float(0.02), round(float(legends.iloc[index]._max), 2)
            step = round((_max - _min) / 3, 2)
            for j in range(3):
                limits.append([round(j * step + _min, 2), round((j + 1) * step + _min - 0.01, 2)])
            limits[len(limits) - 1][1] = _max # rounding probs
            colors = colors_zero
        else:
            if positive_negative_0 == 'n':
                colors = colors_negative
            else:
                colors = colors_positive
            step = round((_max - _min) / _count, 2)
            for j in range(_count):
                limits.append([round(j * step + _min, 2), round((j + 1) * step + _min - 0.01, 2)])
            limits[len(limits) - 1][1] = _max # rounding probs
        for j, limit in enumerate(limits):
            minvalue, maxvalue, color, label = limit[0], limit[1], \
                    colors[j], \
                    labels[j] if _count == 7 else labels[j + 4]
            csv += f"{i},{label},{color},{minvalue},{maxvalue},{int(row_l['indicator_id'])},{j + 1},{label.lower()}\n"
            i += 1
        csv +=f"{i},Dado indisponivel,{undefined},,,{row_l['indicator_id']},{j + 2},Dado indisponivel\n"
        i += 1
    with open(r".\legend.csv", "w", encoding='utf-8') as text_file:
        text_file.write(csv)
    pass

if __name__ == '__main__':
    execute()
