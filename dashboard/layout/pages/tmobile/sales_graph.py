from layout.components.figure import figure
import plotly.graph_objects as go


def get_html(title="", flag=1):
    """
    This function should be replaced with code that consumes a document from the firestore and turns it into the needed graph.
    The flag parameter is only temporary to showcase some options of final rendering.
    Perhaps in the second option the total done that year should be shown instead of what has been done the last period.
    :param title:
    :param flag:
    :return:
    """
    if flag == 1:
        fig = {
            'data': [
                {'x': [-8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8],
                 'y': [187, 166, 236, 196, 141, 154, 170, 152],
                 'type': 'bar', 'name': 'Sales'},
                {'x': [-8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8],
                 'y': [86, 98, 102, 100, 90, 99, 177, 194],
                 'type': 'bar', 'name': u'HASses'},
                {'x': [-8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8],
                 'y': [76, 132, 98, 87, 125, 127, 145, 140],
                 'type': 'bar', 'name': u'Activations'},
                {'x': [-8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8],
                 'y': [0, 0, 0, 0, 0, 0, 0, 0, 0, 286, 234, 186, 18, 5, 1, 0, 0],
                 'type': 'bar', 'name': u'HASses*'},
                {'x': [-8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8],
                 'y': [0, 0, 0, 0, 0, 0, 0, 0, 0, 211, 144, 48, 26, 11, 1, 0, 0],
                 'type': 'bar', 'name': u'Activations*'},
            ],
            'layout': {
                'title': title,
                'tickmode': 'linear'
            }
        }
    else:
        fig = go.Figure([
            go.Scatter(
                x=[-7, -6, -5, -4, -3, -2, -1, 0],
                y=[187, 166, 236, 196, 141, 154, 170, 152],
                name="Sales"

            ),
            go.Scatter(
                x=[-7, -6, -5, -4, -3, -2, -1, 0],
                y=[86, 98, 102, 100, 90, 99, 177, 194],
                name="HAS",
                line=dict(color="red")
            ),
            go.Scatter(
                x=[-7, -6, -5, -4, -3, -2, -1, 0],
                y=[76, 132, 98, 87, 125, 127, 145, 140],
                name="Activations",
                line=dict(color="green")
            ),
            go.Scatter(
                x=[0, 1, 2, 3, 4, 5, 6, 7, 8],
                y=[194, 286, 234, 186, 18, 5, 1, 0, 0],
                name="HAS*",
                line=dict(dash='dash', color="red")
            ),
            go.Scatter(
                x=[0, 1, 2, 3, 4, 5, 6, 7, 8],
                y=[140, 211, 144, 48, 26, 11, 1, 0, 0],
                name="Activations*",
                line=dict(dash='dash', color="green")
            )
        ],
            layout=go.Layout(title=title)
        )
        fig.update_xaxes(
            rangeslider_visible=True)

    return figure(figure=fig)
