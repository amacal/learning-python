import os
import click
import openai

schema = {
    "type": "object",
    "properties": {
        "ingredients": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "unit": {"type": "string", "enum": ["grams", "ml", "cups", "pieces", "teaspoons"]},
                    "amount": {"type": "number"},
                },
                "required": ["name", "unit", "amount"],
            },
        },
        "instructions": {
            "type": "array",
            "description": "Steps to prepare the recipe (no numbering)",
            "items": {"type": "string"},
        },
        "time_to_cook": {"type": "number", "description": "Total time to prepare the recipe in minutes"},
    },
    "required": ["ingredients", "instructions", "time_to_cook"],
}


@click.command()
def gpt():
    openai.api_key = os.getenv("OPENAI_API_KEY")

    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "Provide a recipe for spaghetti bolognese"},
        ],
        functions=[{"name": "set_recipe", "parameters": schema}],
        function_call={"name": "set_recipe"},
        temperature=0,
    )

    print(completion)
