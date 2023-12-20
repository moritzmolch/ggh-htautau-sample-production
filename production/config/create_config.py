import json


config = []


for _higgs_mass in list(range(50, 150)):
    config.append({
        "higgs_mass": _higgs_mass,
        "n_files": 50,
        "n_events_per_file": 2000,
    })

for _higgs_mass in list(range(150, 250)):
    config.append({
        "higgs_mass": _higgs_mass,
        "n_files": 30,
        "n_events_per_file": 2000,
    })

for _higgs_mass in list(range(300, 805, 5)):
    config.append({
        "higgs_mass": _higgs_mass,
        "n_files": 15,
        "n_events_per_file": 4000,
    })


with open("generation_config.json", "w") as f:
    json.dump(config ,f)
