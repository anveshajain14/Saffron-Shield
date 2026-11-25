import json

import mlflow


class ClaimsAgentModel(mlflow.pyfunc.PythonModel):
    """Saffron Shield claims agent in deployable form.

    The system prompt that defines the agent is logged as a model
    artifact ("system_prompt"), so each registered version is
    fingerprinted by the prompt it carries. Answers are produced by the
    Databricks foundation LLM under that prompt, queried through
    mlflow.deployments. The model needs no packages beyond mlflow, so
    Model Serving builds the endpoint quickly (express deployment)
    instead of installing the full agent stack.
    """

    def load_context(self, context):
        from pathlib import Path

        self.system_prompt = (
            Path(context.artifacts["system_prompt"])
            .read_text(encoding="utf-8")
            .strip()
        )

    def _llm_answer(self, messages):
        from mlflow.deployments import get_deploy_client

        client = get_deploy_client("databricks")

        response = client.predict(
            endpoint="databricks-meta-llama-3-3-70b-instruct",
            inputs={
                "messages": messages,
                "temperature": 0,
                "max_tokens": 512,
            },
        )

        # OpenAI-compatible response shape.
        try:
            return response["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError):
            return json.dumps(response)

    def _messages_from_input(self, model_input):
        # Accept a plain question string, {"question": ...}, chat-style
        # {"messages": [...]}, or a single-cell pandas input.
        if isinstance(model_input, str):
            return [{"role": "user", "content": model_input}]

        if isinstance(model_input, dict):
            if "question" in model_input:
                return [
                    {
                        "role": "user",
                        "content": str(model_input["question"]),
                    }
                ]
            if "messages" in model_input:
                return [
                    {
                        "role": m.get("role", "user"),
                        "content": str(m.get("content", "")),
                    }
                    for m in model_input["messages"]
                    if isinstance(m, dict)
                ]
            return [
                {
                    "role": "user",
                    "content": json.dumps(model_input),
                }
            ]

        try:
            # A pandas cell arrived via the scoring server.
            return [
                {"role": "user", "content": str(model_input.iloc[0, 0])}
            ]
        except Exception:
            return [{"role": "user", "content": str(model_input)}]

    def predict(self, context, model_input, params=None):
        messages = [{"role": "system", "content": self.system_prompt}]
        messages += self._messages_from_input(model_input)

        try:
            return self._llm_answer(messages)
        except Exception as error:
            # Keep the endpoint answering even if the LLM backend is
            # unreachable; the message makes the failure visible.
            return (
                "Saffron Shield claims agent is serving, but the "
                "foundation model could not be reached: "
                + str(error)
            )


mlflow.models.set_model(ClaimsAgentModel())
