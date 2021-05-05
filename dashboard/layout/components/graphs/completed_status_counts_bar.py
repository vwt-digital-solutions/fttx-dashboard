import pandas as pd
import plotly.graph_objects as go

from config import colors_vwt as colors


def get_fig(df: pd.DataFrame, title="") -> go.Figure:
    phase_labels = ["Schouwen", "BIS", "Montage-LasAP", "Montage-LasDP", "HAS"]
    fig = go.Figure(
        data=[
            go.Bar(
                name="Opgeleverd HC",
                x=phase_labels,
                y=df[df.status == "opgeleverd"]["count"].values,
                marker=dict(color=colors["green"]),
                customdata=[f"{col};opgeleverd" for col in df.phase.unique()],
            ),
            go.Bar(
                name="Opgeleverd zonder HC",
                x=phase_labels,
                y=df[df.status == "opgeleverd_zonder_hc"]["count"].values,
                marker=dict(color=colors["yellow"]),
                customdata=[f"{col};opgeleverd_zonder_hc" for col in df.phase.unique()],
            ),
            go.Bar(
                name="Ingeplanned",
                x=phase_labels,
                y=df[df.status == "ingeplanned"]["count"].values,
                marker=dict(color=colors["orange"]),
                customdata=[f"{col};ingeplanned" for col in df.phase.unique()],
            ),
            go.Bar(
                name="Niet opgeleverd",
                x=phase_labels,
                y=df[df.status == "niet_opgeleverd"]["count"].values,
                marker=dict(color=colors["red"]),
                customdata=[f"{col};niet_opgeleverd" for col in df.phase.unique()],
            ),
        ]
    )
    fig.update_layout(
        barmode="stack",
        title=title,
        yaxis_title="Aantal woningen",
        height=500,
        plot_bgcolor=colors["plot_bgcolor"],
        paper_bgcolor=colors["paper_bgcolor"],
    )
    return fig
