import openai

async def payroll_transformer(columns, input, model):

    # Replace with your desired model
    client = openai.AsyncOpenAI()

    # Construct the prompt using formatted strings

    prompt = f"""I will give you some column names from a payroll document and I need you to give me the way to map those into the
                output columns. I'll also provide for you with a small explanation/description for each of the output columns.
                You can map more than one column in the input to one in the output if you think they all correspond to the same category.
                I need this response in JSON format.

                Here's an small mock example:

                Input columns: [Tax_1, Tax_2, Income]
                Output columns: [Taxes, Income]

                Mapping: {{Taxes: [Tax_1, Tax_2], Income: [Income]}}

                Now I'll give you the real data you have to work with:

                Input columns: [{input}]


                """

    prompt += f"""Output columns with their descriptions: [{columns}]
                """

    if "Tier1Id" in columns:
        prompt += f"""Tier1id, Tier2id and Tier3id must be found with these exact names in the input columns in order to be mapped
                If there is nothing in the input with those exact names just leave the corresponding fields empty."""


    messages = [
    {"role": "system",
     "content": "You're a useful financial assistant that can understand financial documents with their contents and categories."},
    {"role": "user",
     "content": prompt}
    ]


    response = await client.chat.completions.create(
        model=model,
        messages=messages,
        max_tokens=2000,
        n=1,
        stop=None,
        temperature=0.1,
        response_format={ "type": "json_object" }
    )

    return response.choices[0].message.content

def map_tax_types(input_tax_type, tax_type_list):

    client = openai.OpenAI()

    prompt = f"""I need you to look at an input tax name and map it on to one of the tax types from a list that I'll provide for you.
    Return only the the correct tax type form the list wihtout any additional comment.
    Keep in mind that any of the following taxes should be classified with a tax type of 'ST (State Taxes)':
    - SIT (State Income Tax)
    - SDI (Employee State Disability Insurance Tax)
    - ER_SDI (Employer State Disability Insurance Tax)
    - FLI (Employee Family Leave Insurance Tax)
    - ER_FLI (Employer Family Leave Insurance Tax)
    
    Now I'll provide the input tax name and the complete tax type list.
    
    Here is the input tax name: {input_tax_type}
    And here is the list of tax types: {tax_type_list}"""

    response = client.responses.create(
      model="gpt-4o-mini",
      input=prompt,
      instructions="You are a expert in tax classification."
    )

    return response.output[0].content[0].text

def detect_state_and_local_columns(input, model):

    # Replace with your desired model
    client = openai.OpenAI()

    # Construct the prompt using formatted strings

    prompt = f"""I will give you the column names of a financial document wth information about employees. I need you to identify the columns that correspond to the
    state and the one that contains the city/county. I you think multiple columns could contain the city/county you can return multiple ones for that field.
    If there is no suitable column for any of the fields, return an empty string in the corresponding one.
    I need this in JSON format with two fields: [state_column, city_county_column].
    These are the input columns:
    {input}"""


    messages = [
    {"role": "system",
     "content": "You're a useful financial assistant that can understand financial documents with their contents and categories."},
    {"role": "user",
     "content": prompt}
    ]


    response = client.chat.completions.create(
        model=model,
        messages=messages,
        max_tokens=500,
        n=1,
        stop=None,
        temperature=0.3,
        response_format={ "type": "json_object" }
    )

    response = response.choices[0].message.content

    return response

def get_state_code(input_text):

    client = openai.OpenAI()


    prompt = f"""I will give you some text that might contain the state to which a certain employee belongs to. If it does,
    return only the state code for that state. If it doesn't, return an empty string.
    This is the input text: {input_text}"""


    messages = [
    {"role": "system",
     "content": "You're a useful assistant."},
    {"role": "user",
     "content": prompt}
    ]


    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        max_tokens=500,
        n=1,
        stop=None,
        temperature=0.2
    )

    return response.choices[0].message.content

def get_correct_tax_name(tax_type, tax_name_list, local):

    client = openai.OpenAI()


    prompt = f"""I will give you a label for a certain tax and I need you to map it to the closest tax name from a list I'll provide for you
    Return only the tax name from the list with no additional comment. If there is no clear match or if the answer is ambiguous, return 'None'.
    """
    if isinstance(local, str):
      prompt += f"The employee lives in {local}"

    prompt += f"""Here's the input tax label: {tax_type}
    And here's the list of possible tax names: {tax_name_list}"""


    messages = [
    {"role": "system",
     "content": "You're a useful assistant."},
    {"role": "user",
     "content": prompt}
    ]


    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        max_tokens=500,
        n=1,
        stop=None,
        temperature=0.2
    )

    return response.choices[0].message.content

def get_correct_state_tax_code(tax_type, tax_name_list):

    client = openai.OpenAI()


    prompt = f"""I will give you a label for a certain tax and I need you to map it to the correct tax code form a list I'll provide for you.
    The list will contain a small explanation for each code in a parethesis after the code.
    If none in the list is a match, return an empty string.
    If you find a suitable match, return only the correct tax code from the list without the parenthesis explanation and with no additional comment.
    Here's the input tax label: {tax_type}
    And here's the list of possible tax codes: {tax_name_list}"""


    messages = [
    {"role": "system",
     "content": "You're a useful assistant."},
    {"role": "user",
     "content": prompt}
    ]


    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        max_tokens=500,
        n=1,
        stop=None,
        temperature=0.2
    )


    return response.choices[0].message.content