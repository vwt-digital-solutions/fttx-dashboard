# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import config
sns.set()

# %%
p_path = './Plaatjes/'
files = [config.brielle,
         config.dongen,
         config.helvoirt,
         config.nijmegen]

fn_date = []
df_l = []
project = ['Brielle', 'Dongen', 'Helvoirt', 'Nijmegen Oosterhout']
aantal_woningen = []
aantal_DP = []
aantal_nonactief = []
aantal_civiel_open = []
aantal_lassen_open = []
aantal_schouwen_open = []
aantal_HAS_open = []
for fn in files:
    fn_date += [fn[20:24] + '-' + fn[24:26] + '-' + fn[26:28]]
    df = pd.read_csv(fn, sep=';', encoding='latin-1')
    df[['SchouwDatum', 'Plandatum', 'Opleverdatum']] = df[['SchouwDatum', 'Plandatum', 'Opleverdatum']].astype('datetime64')
    df[['Opleverstatus', 'Status civiel', 'schouwAkkoord']] = df[['Opleverstatus', 'Status civiel', 'schouwAkkoord']].astype(int)
    df['delta_plan_oplever'] = (df['Plandatum'] - df['Opleverdatum']).dt.days
    df_l += [df]

    aantal_woningen += [len(df)]
    aantal_DP += [len(df['DP'].dropna().unique())]
    aantal_nonactief += [aantal_woningen[-1] - df['Opleverstatus'].value_counts()[2]]
    aantal_civiel_open += [aantal_woningen[-1] - df['Status civiel'].sum()]
    aantal_lassen_open += [aantal_woningen[-1] - min(df.LaswerkAPGereed.sum(), df.LaswerkDPGereed.sum())]
    aantal_schouwen_open += [len(df[(df['SchouwDatum'].isna()) & (df['HasApp_Status'] != 'VOLTOOID')])]
    aantal_HAS_open += [aantal_woningen[-1] - df['HasApp_Status'].value_counts()['VOLTOOID']]

    df_detail = df[df['HasApp_Status'] != 'VOLTOOID'][
        ['Sleutel', 'Opleverdatum', 'Opleverstatus', 'RedenNA', 'Toelichting status']].sort_values(by=['Opleverdatum'])
    df_detail.to_excel('Voorbeeld_tabel_redenen_' + project[-1] + '.xlsx')

    print('Totaal aantal woningen in' + project[-1] + ': ' + str(aantal_woningen[-1]))
    print(' ')
    print('Aantal hangt op civiel: ' + str(aantal_civiel_open[-1]))
    print('Aantal hangt op lassen (AP & DP): ' + str(aantal_lassen_open[-1]))
    print('Aantal hangt op schouwen: ' + str(aantal_schouwen_open[-1]))
    print('Aantal hangt op HAS: ' + str(aantal_HAS_open[-1] - aantal_schouwen_open[-1] - aantal_lassen_open[-1] - aantal_civiel_open[-1]))
    print('Woningen actief (status 2): ' + str(aantal_woningen[-1] - aantal_HAS_open[-1]))
    print('------------------------')


# %% pie charts
def autopct_format(values):
    def my_format(pct):
        total = sum(values)
        val = int(round(pct*total/100.0))
        return '{v:d}'.format(v=val)
    return my_format


labels_ov = ['Fase Civiel',
             'Fase Schouwen',
             'Fase HAS',
             'Fase lassen (AP & DP)',
             'Fase actief (status 2)']

fig, axs = plt.subplots(2, 2, figsize=[12, 10])
x_s = [0, 0, 1, 1]
y_s = [0, 1, 0, 1]

for i in range(0, 4):
    y_pie = [aantal_civiel_open[i],
             aantal_schouwen_open[i],
             aantal_HAS_open[i] - aantal_schouwen_open[i] - aantal_lassen_open[i] - aantal_civiel_open[i],
             aantal_lassen_open[i],
             aantal_woningen[i] - aantal_HAS_open[i]]
    explodeTuple = (0.5, 0, 0, 0.3, 0.1)
    axs[x_s[i], y_s[i]].pie(y_pie, autopct=autopct_format(y_pie),
                            explode=explodeTuple, startangle=90, textprops={'fontsize': 14})
    plt_t = project[i]
    axs[x_s[i], y_s[i]].set_title(plt_t, fontsize=14)

fig.legend(labels_ov, loc=10, fontsize=14)
plt.savefig(p_path + 'Alle Projecten' + '.png', facecolor='w')

# %% bar chart specifiek voor HAS, Brielle
df = df_l[0]
df_detail = df[df['HasApp_Status'] != 'VOLTOOID'][
    ['Sleutel', 'Opleverdatum', 'Opleverstatus', 'RedenNA', 'Toelichting status']].sort_values(by=['Opleverdatum'])

status = {}
status[0] = df_detail[df_detail['Opleverstatus'] == 1]['RedenNA'].value_counts()
status[1] = df_detail[df_detail['Opleverstatus'] == 2]['RedenNA'].value_counts()
status[2] = df_detail[df_detail['Opleverstatus'] == 5]['RedenNA'].value_counts()
status[3] = df_detail[df_detail['Opleverstatus'] == 31]['RedenNA'].value_counts()
status[4] = df_detail[df_detail['Opleverstatus'] == 35]['RedenNA'].value_counts()

y_bar = [0, 0, 0, 0, 0]
y_bar_t = []
for i in range(0, 5):
    for key in ['R1', 'R2', 'R3', 'R5', 'R6', 'R7', 'R8', 'R10']:
        if key in status[i]:
            y_bar_t += [status[i][key]]
        else:
            y_bar_t += [0]

    y_bar[i] = y_bar_t
    y_bar_t = []

info_l = dict(R0='Geplande aansluiting',
              R1='Geen toestemming bewoner',
              R2='Geen toestemming VVE / WOCO',
              R3='Bewoner na 3 pogingen niet thuis',
              R4='Nieuwbouw (woning nog niet gereed)',
              R5='Hoogbouw obstructie (blokkeert andere bewoners)',
              R6='Hoogbouw obstructie (wordt geblokkeerd door andere bewoners)',
              R7='Technische obstructie',
              R8='Meterkast voldoet niet aan eisen',
              R9='Pand staat leeg',
              R10='Geen graafvergunning',
              R11='Aansluitkosten boven normbedrag niet gedekt',
              R12='Buiten het uitrolgebied',
              R13='Glasnetwerk van een andere operator',
              R14='Geen vezelcapaciteit',
              R15='Geen woning',
              R16='Sloopwoning (niet voorbereid)',
              R17='Complex met 1 aansluiting op ander adres',
              R18='Klant niet bereikbaar',
              R19='Bewoner niet thuis, wordt opnieuw ingepland',
              R20='Uitrol na vraagbundeling, klant neemt geen dienst',
              R21='Wordt niet binnen dit project aangesloten',
              R22='Vorst, niet planbaar'
              )
labels = ['s1: rol gevel (LB)', 's5: rol erfgrens (LB)', 's31: rol gevel (HB)', 's35: rol erfgrens (HB)']
x = np.arange(len(labels))  # the label locations
width = 0.15  # the width of the bars

y_bar[0][5] = 400
fig, ax = plt.subplots(figsize=[12, 10])

rects1 = ax.bar(x - 16*width/8, [y_bar[0][0], y_bar[2][0], y_bar[3][0], y_bar[4][0]], width, label=info_l['R1'])
rects2 = ax.bar(x - 12*width/8, [y_bar[0][1], y_bar[2][1], y_bar[3][1], y_bar[4][1]], width, label=info_l['R2'])
rects3 = ax.bar(x - 8*width/8, [y_bar[0][2], y_bar[2][2], y_bar[3][2], y_bar[4][2]], width, label=info_l['R3'])
rects4 = ax.bar(x - 4*width/8, [y_bar[0][3], y_bar[2][3], y_bar[3][3], y_bar[4][3]], width, label=info_l['R5'])
rects5 = ax.bar(x + 4*width/8, [y_bar[0][4], y_bar[2][4], y_bar[3][4], y_bar[4][4]], width, label=info_l['R6'])
rects6 = ax.bar(x + 8*width/8, [y_bar[0][5], y_bar[2][5], y_bar[3][5], y_bar[4][5]], width, label=info_l['R7'])
rects7 = ax.bar(x + 12*width/8, [y_bar[0][6], y_bar[2][6], y_bar[3][6], y_bar[4][6]], width, label=info_l['R8'])
rects8 = ax.bar(x + 16*width/8, [y_bar[0][7], y_bar[2][7], y_bar[3][7], y_bar[4][7]], width, label=info_l['R10'])

ax.set_ylabel('Aantal', fontsize=14)
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend(fontsize=14)
ax.tick_params(axis='both', which='major', labelsize=14)
ax.tick_params(axis='both', which='minor', labelsize=14)

plt_t = 'Foutmeldingen per opleverstatus voor fase HAS'
ax.set_title(plt_t, fontsize=14)
plt.savefig(p_path + plt_t + '.png', facecolor='w')

# %%
# fig, axs = plt.subplots(2, 2, figsize=[15, 15])
# x_s = [0, 0, 1, 1]
# y_s = [0, 1, 0, 1]

# for i in range(0, 4):
#     df = df_l[i]
#     data = df['Team'].value_counts()
#     labels = []
#     counts = []
#     for key, value in data.items():
#         labels += [key]
#         counts += [value]
#     axs[x_s[i], y_s[i]].bar()
#     plt_t = project[i]
#     axs[x_s[i], y_s[i]].set_title(plt_t + ' 2019', fontsize=16)

# for ax in axs.flat:
#     ax.set_xlabel('Maand', fontsize=16)
#     ax.tick_params(axis='both', which='major', labelsize=16)
#     ax.tick_params(axis='both', which='minor', labelsize=16)

# fig.suptitle('Overzicht opleveringen per project 2019', fontsize=20)
# plt.savefig(p_path + 'Alle Projecten opleveringen' + '.png', facecolor='w')


# %%
df = df_l[0]
fig, axs = plt.subplots(2, 2, figsize=[14, 14])
x_s = [0, 0, 1, 1]
y_s = [0, 1, 0, 1]

for i in range(0, 4):
    df = df_l[i]
    df = df[(df['SchouwDatum'] >= '2019-01-01') & (df['SchouwDatum'] <= '2019-12-31')]
    axs[x_s[i], y_s[i]].hist(df['SchouwDatum'].dropna().dt.month)
    plt_t = project[i]
    axs[x_s[i], y_s[i]].set_title(plt_t + ' 2019')

for ax in axs.flat:
    ax.set(xlabel='Maand')

fig.suptitle('Overzicht schouwingen per project 2019')
plt.savefig(p_path + 'Alle Projecten schouwingen' + '.png', facecolor='w')

# %%
df = df_l[0].append(df_l[1]).append(df_l[2]).append(df_l[3])
df = df[(df['SchouwDatum'] >= '2019-01-01') & (df['SchouwDatum'] <= '2019-12-31')]
fig, axs = plt.subplots(figsize=[12, 10])
axs.hist(df['SchouwDatum'].dropna().dt.month)
axs.set_title('Alle projecten schouwingen 2019')
axs.set(xlabel='Maand')

plt.savefig(p_path + 'Alle Projecten schouwingen in 1' + '.png', facecolor='w')

# %%
df = df_l[0]
fig, axs = plt.subplots(2, 2, figsize=[15, 15])
x_s = [0, 0, 1, 1]
y_s = [0, 1, 0, 1]

for i in range(0, 4):
    df = df_l[i]
    df = df[df['Opleverstatus'] == 2]
    df = df[(df['Opleverdatum'] >= '2019-01-01') & (df['Opleverdatum'] <= '2019-12-31')]
    axs[x_s[i], y_s[i]].hist(df['Opleverdatum'].dropna().dt.month)
    plt_t = project[i]
    axs[x_s[i], y_s[i]].set_title(plt_t + ' 2019', fontsize=16)

for ax in axs.flat:
    ax.set_xlabel('Maand', fontsize=16)
    ax.tick_params(axis='both', which='major', labelsize=16)
    ax.tick_params(axis='both', which='minor', labelsize=16)

fig.suptitle('Overzicht opleveringen per project 2019', fontsize=20)
plt.savefig(p_path + 'Alle Projecten opleveringen' + '.png', facecolor='w')

# %%
df = df_l[0].append(df_l[1]).append(df_l[2]).append(df_l[3])
df = df[df['Opleverstatus'] == 2]
df = df[(df['Opleverdatum'] >= '2019-01-01') & (df['Opleverdatum'] <= '2019-12-31')]
fig, axs = plt.subplots(figsize=[12, 10])
axs.hist(df['Opleverdatum'].dropna().dt.month)
axs.set_title('Alle projecten opleveringen 2019')
axs.set(xlabel='Maand')

plt.savefig(p_path + 'Alle Projecten opleveringen in 1' + '.png', facecolor='w')

# %%
df = df_l[0].append(df_l[1]).append(df_l[2]).append(df_l[3])
df_1 = df[(df['Opleverstatus'] == 2) & (df['Opleverdatum'] >= '2019-01-01') & (df['Opleverdatum'] <= '2019-12-31')]
df_2 = df[(df['SchouwDatum'] >= '2019-01-01') & (df['SchouwDatum'] <= '2019-12-31')]
s1 = df_1['Opleverdatum'].dropna().dt.month.to_list()
s2 = df_2['SchouwDatum'].dropna().dt.month.to_list()

fig, axs = plt.subplots(figsize=[12, 10])
axs.hist(s1 + s2)
axs.set_title('Alle projecten schouwingen & opleveringen 2019')
axs.set(xlabel='Maand')

plt.savefig(p_path + 'Alle Projecten schouwingen en opleveringen in 1' + '.png', facecolor='w')


# %%
# deze twee histogrammen kunnen verder uitgesplitst over teams!

# fig2 = plt.hist(df['Plandatum'].dt.month)
# plt.title('Aantal planningen per maand in ' + project)

# # planning vs oplevering
# b1 = len(df[(df['delta_plan_oplever'] <= -15)])
# b2 = len(df[(df['delta_plan_oplever'] <= -2) & (df['delta_plan_oplever'] >= -14)])
# b3 = len(df[(df['delta_plan_oplever'] <= 1) & (df['delta_plan_oplever'] >= -1)])
# b4 = len(df[(df['delta_plan_oplever'] <= 14) & (df['delta_plan_oplever'] >= 2)])
# b5 = len(df[(df['delta_plan_oplever'] >= 15)])
# plt.figure(5, figsize=[6, 6])
# plt.bar(['(x <=-15)', '(-14 <= x <=-2)', '(-1 <= x <= 1)', '(2 <= x <= 14)', '(x >= 15)'],
#                 np.divide([b1, b2, b3, b4, b5],sum([b1, b2, b3, b4, b5])))
# plt_t = 'Verschil plan- en opleverdatum in ' + project
# plt.title(plt_t)
# plt.savefig(p_path + plt_t + '.png', facecolor='w'

# plt.figure(2, figsize=[12, 6])
# x_bar = labels_ov[0:4]
# y_bar = [aantal_civiel_open, aantal_lassen_open, aantal_schouwen_open, aantal_HAS_open]
# plt.bar(x_bar, y_bar)
# plt_t = 'Aantal open woningen per fase in ' + project
# plt.title(plt_t)
# plt.savefig(p_path + plt_t + '.png', facecolor='w')
