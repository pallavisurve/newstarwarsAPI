"""
1. TODO - import all resource classes here
2. TODO - get count of each resource
3. TODO - get singular resource URL from each resource
    - for example,
    - hit plural URL of starships and that will list all starships data
    - iterate on each starship data and capture singular URLs
    - all_starship_data = data.get("results")
    - you will iterate on `all_starship_data`,
4. TODO - pull data from random 3 "singular" resource URLs
    - utilize`utils` package to produce random 3 numbers from resource ids
    - and pull data for them
5. TODO - convert the script into CLI application
6. TODO - pretty print output (from pprint import pprint)
"""

# resource classes
from resources.films import Film
from resources.characters import Character
from resources.planets import Planet
from resources.species import Species
from resources.starships import Starship
from resources.vehicles import Vehicle

# pydantic classes (models)
from models.datamodels.characters import Character_
from models.datamodels.films import Film_
from models.datamodels.planets import Planet_
from models.datamodels.starships import Starship_
from models.datamodels.vehicles import Vehicle_
from models.datamodels.species import Species_


if __name__ == "__main__":

    character_data = Character().get_sample_data()
    character_data = Character_(**character_data)

    film_data = Film().get_sample_data()
    film_data = Film_(**film_data)
    breakpoint()
    planets_data = Planet().get_sample_data()
    planets_data = Planet_(**planets_data)

    species_data = Species().get_sample_data()
    species_data = Species_(**species_data)

    starships_data = Starship().get_sample_data(id_=9)
    starships_data = Starship_(**starships_data)

    vehicles_data = Vehicle().get_sample_data(id_=4)
    vehicles_data = Vehicle_(**vehicles_data)

    breakpoint()
