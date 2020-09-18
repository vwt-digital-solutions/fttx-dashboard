from layout.components.figure import figure
import plotly.graph_objects as go


def get_html_week(data):
    return figure(
        graph_id="week-overview",
        figure=go.Figure(
            data=[
                go.Bar(name="Geplande HASsen", x=data.date, y=data.count_hasdatum, customdata=list(range(len(data.date)))),
                go.Bar(name="Uitgevoerde HASsen", x=data.date, y=data.count_opleverdatum),
            ],
            layout={
                "barmode": 'group',
                "title": "Geplande HASsen en uitgevoerde HASsen per week",
                "yaxis": {"title": "Aantal"},
                "xaxis": {"title": "Week"},
                "clickmode": 'event'
            }
        )
    )


def get_html_month(data):
    return figure(
        graph_id="month-overview",
        figure=go.Figure(
            data=[
                go.Bar(name="Geplande HASsen", x=data.date, y=data.count_hasdatum),
                go.Bar(name="Uitgevoerde HASsen", x=data.date, y=data.count_opleverdatum),
            ],
            layout={
                "barmode": 'group',
                "title": "Geplande HASsen en uitgevoerde HASsen per maand",
                "yaxis": {"title": "Aantal"},
                "xaxis": {"title": "Maand"},
                "clickmode": 'event'
            }
        )
    )
