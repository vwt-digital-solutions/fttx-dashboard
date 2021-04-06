import dash_table
import pandas as pd

from elements import table_styles


def get_ftu_table(data):
    df = pd.DataFrame()
    df["Project"] = data["FTU0"].keys()
    for key, value in data.items():
        df[key] = value.values()
    df.replace("None", "", inplace=True)
    table = dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in df.columns],
        data=df.to_dict("rows"),
        filter_action="native",
        sort_action="native",
        style_table={"overflowX": "auto", "overflowY": "auto"},
        style_header=table_styles["header"],
        style_cell=table_styles["cell"]["action"],
        style_filter=table_styles["filter"],
        css=[{"selector": "table", "rule": "width: 100%;"}],
    )

    return table
