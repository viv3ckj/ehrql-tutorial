from ehrql import create_dataset, days
from ehrql.tables.tpp import patients, practice_registrations

#create dataset
dataset = create_dataset()

index_date = "2023-10-01"

is_female_or_male = patients.sex.is_in(["male", "female"])

was_adult = (patients.age_on(index_date) >= 18) & (
    patients.age_on(index_date) <= 110
)

was_alive = (
    patients.date_of_death.is_after(index_date)
    | patients.date_of_death.is_null()
    )

was_registered = practice_registrations.for_patient_on(
    index_date
).exists_for_patient()

dataset.define_population(
    is_female_or_male
    & was_adult
    & was_alive
    & was_registered
)

from ehrql import codelist_from_csv

dataset.sex = patients.sex
dataset.age = patients.age_on(index_date)

ethnicity_codelist = codelist_from_csv(
    "codelists/opensafely-ethnicity.csv",
    column="Code",
    category_column="Grouping_6",
)

from ehrql.tables.tpp import clinical_events
dataset.ethnicity = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(ethnicity_codelist)
    )
    .sort_by(clinical_events.date)
    .last_for_patient()
    .ctv3_code.to_category(ethnicity_codelist)
)

from ehrql import case, when
from ehrql.tables.tpp import addresses

imd_rounded = addresses.for_patient_on(
    index_date
).imd_rounded
max_imd = 32844
dataset.imd_quintile = case(
    when(imd_rounded < int(max_imd * 1 / 5)).then(1),
    when(imd_rounded < int(max_imd * 2 / 5)).then(2),
    when(imd_rounded < int(max_imd * 3 / 5)).then(3),
    when(imd_rounded < int(max_imd * 4 / 5)).then(4),
    when(imd_rounded <= max_imd). then(5),
)

asthma_inhaler_codelist = codelist_from_csv(
    "codelists/opensafely-asthma-inhaler-salbutamol-medication.csv",
    column="code",
)
from ehrql.tables.tpp import medications

dataset.num_asthma_inhaler_medications = medications.where(
    medications.dmd_code.is_in(asthma_inhaler_codelist)
    & medications.date.is_on_or_between(
        index_date - days(30), index_date
    )
).count_for_patient()

from ehrql.tables.tpp import apcs

dataset.date_of_first_admission = (
    apcs.where(
        apcs.admission_date.is_after(
            index_date
        )
    )
    .sort_by(apcs.admission_date)
    .first_for_patient()
    .admission_date
)