from Analyse.Indicators.Indicator import Indicator
# Todo: Move the dependence on color to front-end
import config
colors = config.colors_vwt


class PieChart(Indicator):

    @staticmethod
    def get_pie_layout():
        """
        Getter for the layout of the reden_na pie chart
        Returns: Layout for reden_na pie chart.

        """
        layout = {
            #   'clickmode': 'event+select',
            'showlegend': True,
            'autosize': True,
            'margin': {'l': 50, 'r': 50, 'b': 100, 't': 100},
            'title': {'text': 'Opgegeven reden na'},
            'height': 500,
            'plot_bgcolor': colors['plot_bgcolor'],
            'paper_bgcolor': colors['paper_bgcolor'],
        }
        return layout

    @staticmethod
    def to_pie_chart(df):
        """
        Function to change aggregate into a dictionary for reden na, should be refactored to only write values
        to firestore, instead of including layout.
        Args:
            df:

        Returns:

        """
        labels = df.index.to_list()
        values = df.sleutel.to_list()

        return {
            'labels': labels,
            'values': values,
            'marker': {
                'colors':
                    [
                        colors['vwt_blue'],
                        colors['yellow'],
                        colors['red'],
                        colors['green']
                    ]
            }
        }
