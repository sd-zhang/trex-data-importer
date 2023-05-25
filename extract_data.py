# assume year is 2016, as it is the last time before 2022 with a July 31 falling on a Sunday
# make start time August 1, 0:0:0 to conform to 0 index time (i.e, July 31, hour 24 = August 1, hour 0)
# assume timezone is Arizona, since there is no DST
import dataset

def extract_energy(file_name, **kwargs):
    # read energy profile
    with open(file_name, 'r') as f:
        # headers = []
        data = []

        # read header
        headers = f.readline().lower().strip().rstrip('\n').replace('"','').split(',')
        use_col = 7
        gen_col = 11

        # starting time is August 1, 2016, 12:00 AM, phoenix time
        timestamp = 1470034800

        while True:
            line = f.readline().strip().rstrip('\n').split(',')

            if not line[0]:
                break

            # convert load from kWh to Wh
            consumption = float(line[use_col]) * 1000
            # generation is assumed to be the average Watts over an hour, which is the same quantity in Wh
            generation = float(line[gen_col])

            data_row = {'time': timestamp,
                        'consumption': consumption,
                        'generation': generation}
            data.append(data_row)

            timestamp += 3600
        return data

if __name__ == "__main__":
    file_dir = "dataverse_files/citylearn_challenge_2022_phase_all/"
    profile_name = "Building_1"

    db = dataset.connect("postgresql://postgres:postgres@localhost/citylearn")
    energy_profile = extract_energy(file_dir + profile_name + ".csv")
        if energy_profile:
            if profile_name not in db.tables:
                table = db.create_table(profile_name, primary_id='time', primary_type=db.types.integer)
            else:
                table = db[profile_name]

            table.upsert_many(energy_profile, ['time'])