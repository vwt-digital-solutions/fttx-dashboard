from layout.components.figure import figure
import plotly.graph_objects as go


def get_html_week(data):
    return figure(
        figure=go.Figure(
            data=[
                go.Bar(name="Geplande HASsen", x=data.date, y=data.count_plandatum),
                go.Bar(name="Uitgevoerde HASsen", x=data.date, y=data.count_hasdatum),
            ],
            layout={
                "barmode": 'group',
                "title": "Geplande HASsen en Aantal HASsen per week",
                "yaxis": {"title": "Aantal"},
                "xaxis": {"title": "Week"}
            }
        )
    )


def get_html_month(data):
    return figure(
        figure=go.Figure(
            data=[
                go.Bar(name="Geplande HASsen", x=data.date, y=data.count_plandatum),
                go.Bar(name="Uitgevoerde HASsen", x=data.date, y=data.count_hasdatum),
            ],
            layout={
                "barmode": 'group',
                "title": "Geplande HASsen en Aantal HASsen per maand",
                "yaxis": {"title": "Aantal"},
                "xaxis": {"title": "Maand"}
            }
        )
    )
