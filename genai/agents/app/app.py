import os

import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
from databricks.sdk import WorkspaceClient

# The serving endpoint is discovered from the app's own configuration:
# app.yaml maps the SERVING_ENDPOINT environment variable to the
# serving-endpoint resource. No token and no endpoint name are written
# into this code.
SERVING_ENDPOINT = os.environ["SERVING_ENDPOINT"]

app = dash.Dash(__name__)
server = app.server
app.title = "Saffron Shield Claims Agent"


def ask_agent(question: str) -> str:
    """Send the adjuster's question to the claims agent endpoint.

    The request runs as the app's service principal, which holds
    CAN_QUERY on the endpoint through the app's serving-endpoint
    resource.
    """
    workspace = WorkspaceClient()
    response = workspace.serving_endpoints.query(
        name=SERVING_ENDPOINT,
        dataframe_records=[{"question": question}],
    )
    return response.predictions


def bubble(text, who):
    return html.Div(
        str(text),
        style={
            "background": "#e8f0fe" if who == "user" else "#f1f3f4",
            "padding": "10px 14px",
            "borderRadius": "12px",
            "maxWidth": "70%",
            "whiteSpace": "pre-wrap",
            "alignSelf": "flex-end" if who == "user" else "flex-start",
        },
    )


app.layout = html.Div(
    [
        html.H2("Saffron Shield Claims Agent"),
        html.Div(
            f"Ask claims and policy questions. Answers come from "
            f"endpoint '{SERVING_ENDPOINT}' (from app configuration).",
            style={"color": "#666", "marginBottom": "12px"},
        ),
        html.Div(
            id="conversation",
            style={
                "display": "flex",
                "flexDirection": "column",
                "gap": "8px",
                "minHeight": "300px",
                "padding": "10px",
                "border": "1px solid #ddd",
                "borderRadius": "8px",
                "marginBottom": "10px",
            },
        ),
        html.Div(
            [
                dcc.Input(
                    id="question",
                    type="text",
                    placeholder="Ask about a claim or the policy...",
                    style={"flex": "1", "padding": "10px"},
                ),
                html.Button(
                    "Send",
                    id="send",
                    n_clicks=0,
                    style={"padding": "10px 16px", "marginLeft": "8px"},
                ),
            ],
            style={"display": "flex"},
        ),
    ],
    style={
        "maxWidth": "720px",
        "margin": "40px auto",
        "fontFamily": "sans-serif",
    },
)


@app.callback(
    Output("conversation", "children"),
    Input("send", "n_clicks"),
    State("question", "value"),
    State("conversation", "children"),
    prevent_initial_call=True,
)
def on_send(n_clicks, question, history):
    history = list(history or [])
    if not question or not question.strip():
        return history

    try:
        answer = ask_agent(question.strip())
    except Exception as error:
        answer = f"The agent could not be reached: {error}"

    history.append(bubble(question, "user"))
    history.append(bubble(answer, "agent"))
    return history


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
