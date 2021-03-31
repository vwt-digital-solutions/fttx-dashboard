# %%
import matplotlib.pyplot as plt
import seaborn as sns
import os
import numpy as np
from analyse.functions import get_data_FC, get_data_meters
sns.set()

# %% Set environment variables and permissions and data path
path_jsons = ''
path_data = ''
keys = os.listdir(path_jsons)
for fn in keys:
    if ('-d-' in fn) & ('-fttx-' in fn):
        gpath_d = path_jsons + fn
    if '-p-' in fn:
        gpath_p = path_jsons + fn
    if ('-d-' in fn) & ('-it-' in fn):
        gpath_i = path_jsons + fn

# %%
subset_KPN_2020 = ['Arnhem Gulden Bodem Schaarsbergen',
                   'Arnhem Klarendal',
                   'Arnhem Malburgen',
                   'Arnhem Spijkerbuurt',
                   'Bavel',
                   'Bergen op Zoom Oost',
                   'Bergen op Zoom oude stad',
                   'Bergen op Zoom Noord  wijk 01 + Halsteren',
                   'Breda Brabantpark',
                   'Breda Tuinzicht',
                   'Brielle',
                   #    'Den Haag Cluster B',
                   'Den Haag - Haagse Hout-Bezuidenhout West',
                   'Den Haag Morgenstond west',
                   #    'Den Haag Regentessekwatier',
                   'Den Haag - Vrederust en Bouwlust',
                   #    'Den Haag',
                   'Helvoirt POP Volbouw',
                   'KPN Gouda Kort Haarlem en Noord',
                   'Gouda Centrum',
                   'KPN Spijkernisse',
                   #    'LCM project',
                   'Nijmegen Biezen-Wolfskuil-Hatert ',
                   'Nijmegen Bottendaal',
                   'Nijmegen Dukenburg',
                   'Nijmegen Oosterhout']
col = ['hasdatum', 'id', 'laswerkapgereed', 'laswerkdpgereed', 'opleverdatum',
       'opleverstatus', 'project', 'redenna', 'sleutel', 'soort_bouw',
       'toestemming', 'x_locatie_dp', 'x_locatie_rol', 'y_locatie_dp',
       'y_locatie_rol']
df_l, t_s, x_d, tot_l = get_data_FC(subset_KPN_2020, col, gpath_i, path_data)
d_sheets = get_data_meters(path_data)

# %% kleine analyse meters
BIST = {}
BIS = {}
HAS = {}
n_gereed_BIS = {}
m_BIS_gem = {}
m_HAS_gem = {}
r_his = {}
m_bb = {}
w_HP = {}
w_2 = {}
w_1 = {}
w_31 = {}
w_33 = {}
rat_BIS = {}
x = []
y = []
r = []
z = {}
ar = {}
for key in d_sheets:
    BIST[key] = d_sheets[key].iloc[10, 1]
    BIS[key] = d_sheets[key].iloc[10, 2:].fillna(0)
    HAS[key] = d_sheets[key].iloc[11, 2:].fillna(0)
    n_gereed_BIS[key] = d_sheets[key].iloc[12, 2:].fillna(0)
    rat_BIS[key] = BIS[key] / n_gereed_BIS[key]
    w_HP[key] = d_sheets[key].iloc[28, 2:].fillna(0)
    w_33[key] = d_sheets[key].iloc[27, 2:].fillna(0)
    w_31[key] = d_sheets[key].iloc[25, 2:].fillna(0)
    w_2[key] = d_sheets[key].iloc[24, 2:].fillna(0)
    w_1[key] = d_sheets[key].iloc[22, 2:].fillna(0)
    if (w_HP[key].sum()) / tot_l[key] > 0.2:
        m_BIS_gem[key] = ((BIS[key].cumsum()) / n_gereed_BIS[key].cumsum())[-1]
        m_HAS_gem[key] = ((HAS[key].cumsum()) / (w_HP[key]).cumsum())[-1]
    #     m_HAS_gem[key] = ((HAS[key].cumsum()) / n_gereed_BIS[key].cumsum())[-1]
    #     # x += [m_BIS_gem[key]]
        # y += [tot_l[key]]

        df = df_l[key]
        z[key] = [tot_l[key] / len(df['x_locatie_dp'].unique())]

        m_b = []
        ar[key] = (df['x_locatie_rol'].str.replace(',', '.').astype('float').max()
                   - df['x_locatie_rol'].str.replace(',', '.').astype('float').min()) * \
                  (df['y_locatie_rol'].str.replace(',', '.').astype('float').max()
                   - df['y_locatie_rol'].str.replace(',', '.').astype('float').min())
        ar[key] = ar[key] / len(df['x_locatie_dp'].unique())
        for mask in df['x_locatie_dp'].unique():
            df_s = df[df['x_locatie_dp'] == mask]
            if not df_s.empty:
                xR = df_s['x_locatie_rol'].str.replace(',', '.').astype('float')
                yR = df_s['y_locatie_rol'].str.replace(',', '.').astype('float')
                xDP = df_s['x_locatie_dp'].str.replace(',', '.').astype('float')
                yDP = df_s['y_locatie_dp'].str.replace(',', '.').astype('float')
                r1 = [0]
                r2 = [0]
                r3 = [0]
                r4 = [0]
                for i, el in enumerate(xR):
                    a = xR.iloc[i] - xDP.iloc[i]
                    b = yR.iloc[i] - yDP.iloc[i]
                    if a > 0 and b > 0:
                        r1 += [np.sqrt((a**2 + b**2))]
                    if a > 0 and b < 0:
                        r2 += [np.sqrt((a**2 + b**2))]
                    if a < 0 and b > 0:
                        r3 += [np.sqrt((a**2 + b**2))]
                    if a < 0 and b < 0:
                        r4 += [np.sqrt((a**2 + b**2))]

                # m_b += [r.max() / len(r)]
                m_b += [max(r1) + max(r2) + max(r3) + max(r4)]
        m_b = sum(m_b) / len(m_b)
        m_bb[key] = m_b
    # r += [r_his[key].std()]

# BIS
x_c = ar
# x_c = m_bb
z1 = np.polyfit(list(x_c.values()), list(m_BIS_gem.values()), 1)
x = np.array(list(range(0, int(max(x_c.values())) + 1)))
y = z1[1] + z1[0] * x
# sum(list(m_BIS_gem.values())) / len(list(m_BIS_gem.values())) # gem aantal m BIS per woning
plt.plot(list(x_c.values()), list(m_BIS_gem.values()), 'x')
plt.plot(x, y, '-')
plt.xlabel('Oppervlakte per DP (berekend vanuit FC)')
plt.ylabel('Meters BIS per woning (berekend vanuit excel)')
plt.xlim([0, 40000])
plt.ylim([0, 9])
plt.savefig('Graphs/correlatieBIS.png')
plt.show()

for key in x_c:
    e = (x_c[key] * z1[0] + z1[1]) * n_gereed_BIS[key].cumsum()[-1]  # meter BIS per woning
    dp = (e - BIS[key].cumsum()[-1]) / BIS[key].cumsum()[-1]
    dw = (e - BIS[key].cumsum()[-1]) / n_gereed_BIS[key].cumsum()[-1]
    print(key + ' ' + str(round(abs(dp * 100))) + ' '
          + str(round(n_gereed_BIS[key].cumsum()[-1] / tot_l[key] * 100)))  # meet en schattigsfout !

# HAS
# x_c = m_BIS_gem.values()
z1 = np.polyfit(list(x_c.values()), list(m_HAS_gem.values()), 1)
x = np.array(list(range(0, int(max(x_c.values())) + 1)))
y = z1[1] + z1[0] * x
# plt.plot(list(m_BIS_gem.values()), list(m_HAS_gem.values()), 'x')
plt.plot(list(x_c.values()), list(m_HAS_gem.values()), 'x')
plt.plot(x, y, '-')
plt.xlabel('Oppervlakte per DP (berekend vanuit FC)')
plt.ylabel('Meters HAS per woning (berekend vanuit excel)')
plt.xlim([0, 40000])
plt.ylim([0, 9])
plt.savefig('Graphs/correlatieHAS.png')

for key in x_c:
    e = (x_c[key] * z1[0] + z1[1]) * w_HP[key].cumsum()[-1]  # meter BIS per woning
    dp = (e - HAS[key].cumsum()[-1]) / HAS[key].cumsum()[-1]
    dw = (e - HAS[key].cumsum()[-1]) / w_HP[key].cumsum()[-1]
    print(key + ' ' + str(round(abs(dp * 100))) + ' ' + str(round(w_HP[key].cumsum()[-1] / tot_l[key] * 100)))  # meet en schattigsfout !

# %%
