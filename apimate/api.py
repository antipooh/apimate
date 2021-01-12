from pydantic import BaseModel


def camelize(string: str) -> str:
    assert isinstance(string, str), 'Input must be of type str'

    first_alphabetic_character_index = -1
    for index, character in enumerate(string):
        if character.isalpha():
            first_alphabetic_character_index = index
            break

    empty = ''

    if first_alphabetic_character_index == -1:
        return empty

    string = string[first_alphabetic_character_index:]

    titled_string_generator = (character for character in string.title() if character.isalnum())

    try:
        return next(titled_string_generator).lower() + empty.join(titled_string_generator)

    except StopIteration:
        return empty


class APIModel(BaseModel):
    class Config:
        alias_generator = camelize
        allow_population_by_field_name = True


