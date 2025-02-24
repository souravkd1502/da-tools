import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, State
import plotly.express as px
import pandas as pd

class DashDashboard:
    def __init__(self, execution_plan: dict, data: pd.DataFrame):
        """
        Initializes the Dash Dashboard class with improved UI and interactivity.
        
        :param execution_plan: Dictionary defining charts, filters, and metrics
        :param data: Pandas DataFrame containing the dataset
        """
        self.execution_plan = execution_plan
        self.data = data
        self.app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        self.create_layout()
    
    def create_layout(self):
        """
        Creates an enhanced layout with a sidebar for filters, better responsiveness, and a reset button.
        """
        filters = self.execution_plan.get("filters", [])
        charts = self.execution_plan.get("charts", [])

        sidebar = dbc.Card([
            html.H4("Filters", className="card-title"),
            *[
                html.Div([
                    html.Label(f"Filter by {f}"),
                    dcc.Dropdown(
                        id=f"filter-{f}",
                        options=[{"label": str(val), "value": str(val)} for val in self.data[f].dropna().unique() if val is not None],
                        multi=True
                    )
                ]) for f in filters
            ],
            html.Button("Reset Filters", id="reset-btn", className="btn btn-warning mt-3")
        ], body=True, className="mb-3")

        self.app.layout = dbc.Container([
            dbc.Row([html.H1(self.execution_plan.get("title", "Dashboard"))], className="mb-3"),
            dbc.Row([
                dbc.Col(sidebar, width=3),
                dbc.Col([dcc.Loading(
                    html.Div([dcc.Graph(id=f"chart-{i}") for i in range(len(charts))]),
                    type="circle")], width=9)
            ])
        ], fluid=True)

        self.setup_callbacks()
    
    def setup_callbacks(self):
        """
        Defines Dash callbacks for dynamic filtering and reset functionality.
        """
        inputs = [Input(f"filter-{f}", "value") for f in self.execution_plan.get("filters", [])]
        
        @self.app.callback(
            [Output(f"chart-{i}", "figure") for i in range(len(self.execution_plan.get("charts", [])))],
            inputs
        )
        def update_charts(*filter_values):
            filtered_data = self.data.copy()

            # Apply filters
            for i, f in enumerate(self.execution_plan.get("filters", [])):
                if filter_values[i]:
                    filtered_data = filtered_data[filtered_data[f].isin(filter_values[i])]

            figures = []
            for chart in self.execution_plan["charts"]:
                x_axis = chart["x_axis"]
                y_axis = chart.get("y_axis")  # Some charts (like Pie) don't need y_axis
                
                # Validate column names
                if x_axis not in filtered_data.columns:
                    print(f"Warning: Column '{x_axis}' not found in data.")
                    continue
                if y_axis and y_axis not in filtered_data.columns:
                    print(f"Warning: Column '{y_axis}' not found in data. Selecting first numerical column.")
                    y_axis = next((col for col in filtered_data.select_dtypes(include=['number']).columns), None)
                    if not y_axis:
                        continue  # Skip chart if no numeric column is found
                
                # Generate charts dynamically
                if chart["type"] == "bar":
                    fig = px.bar(filtered_data, x=x_axis, y=y_axis, title=f"{x_axis} vs {y_axis}")
                elif chart["type"] == "line":
                    fig = px.line(filtered_data, x=x_axis, y=y_axis, title=f"{x_axis} vs {y_axis}")
                elif chart["type"] == "pie":
                    fig = px.pie(filtered_data, names=x_axis, values=y_axis, title=f"{x_axis} Distribution")
                else:
                    fig = px.scatter(filtered_data, x=x_axis, y=y_axis, title=f"{x_axis} vs {y_axis}")

                figures.append(fig)

            return figures

        
        @self.app.callback(
            [Output(f"filter-{f}", "value") for f in self.execution_plan.get("filters", [])],
            [Input("reset-btn", "n_clicks")],
            prevent_initial_call=True
        )
        def reset_filters(n_clicks):
            return [None] * len(self.execution_plan.get("filters", []))
    
    def run(self):
        """
        Runs the improved Dash app.
        """
        self.app.run_server(debug=True)
