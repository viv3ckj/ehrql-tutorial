from ehrql import create_dataset
from ehrql.tables.core import patients, medications

#create dataset
dataset = create_dataset()

#definine which patients to be included in dataset
dataset.define_population(patients.date_of_birth.is_on_or_before("1999-12-31"))

#selecting each patients' most recent asthma medication
asthma_codes = ["39113311000001107", "39113611000001102"]
latest_asthma_med = (
    medications.where(medications.dmd_code.is_in(asthma_codes))
    .sort_by(medications.date)
    .last_for_patient()
)

#adding date and code columns to dataset
dataset.asthma_med_date = latest_asthma_med.date
dataset.asthma_med_code = latest_asthma_med.dmd_code