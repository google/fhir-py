# FHIR Views

## Introduction

FHIR Views is a way to define simple, tabular views over complex FHIR data and
turn them into queries that use
[SQL on FHIR conventions](https://github.com/FHIR/sql-on-fhir), or other data
sources in the future. It is installed as part of a simple `pip install
google-fhir-views[r4,bigquery]` command.

FHIR Views has two main concepts:

*   A view *definition*, which defines the fields and criteria created by a
    view.
*   A view *runner*, which creates that view over some data source.

For example, let's create a simple view of patient resources for patients born
before 1960:

```py
import datetime
from google.fhir.views import bigquery_runner, r4

# Uses resource definitions from a public server
views = r4.from_fhir_server('http://hapi.fhir.org/baseR4')

# Creates a view using the base patient profile.
pat = views.view_of('Patient')

example_patients = pat.select({
      'given' : pat.name.given,
      'state' : pat.address.state,
      'family' : pat.name.family,
      'birthDate': pat.birthDate
     }).where(
       pat.birthDate < datetime.date(1960,1,1))
```

If you run the above in a Jupyter notebook or similar tool, you'll notice that
the view builder supports tab suggestions that matches the fields in the FHIR
resource of the given profile. In fact, these expressions are actually FHIRPath
expressions -- simply defined in a fluent Python builder so they get support
like this in notebooks.

Now that we've defined a view, let's run it against a real dataset. We'll run
this over BigQuery:

```py
# Get a BigQuery client. This may require additional authentication to access
# BigQuery, depending on your notebook environment.
from google.cloud import bigquery as bq
client = bq.Client()
runner = bigquery_runner.BigQueryRunner(
    client,
    fhir_dataset='hcls-testing-data.fhir_20k_patients_analytics')

patients_df = runner.to_dataframe(example_patients)
```

That's it! Now the patients_df contains a table of the example patients
described in the query, pulled from the FHIR data stored in BigQuery.

At this time we support a BigQuery runner to consume FHIR data in BigQuery as
our data source, but future runners may support other data stores, FHIR servers,
or FHIR bulk extracts on disk.

Of course, we recommend using a virtualenv and other common Python management
patterns.

## Working with code values

Most meaningful analysis of healthcare data involves navigating clinical
terminologies. In some cases these value sets come from an established authority
like the [Value Set Authority Center](https://vsac.nlm.nih.gov/), and other
times they are defined and maintained locally for custom use cases.

FHIR Views offers a convenient mechanism to create and use such value sets in
your queries. Here is an example that defines a collection of LOINC codes
indicating HbA1c results:

```py
hba1c_value_set = r4.value_set('urn:example:value_set:hba1c').with_codes(
    'http://loinc.org', ['4548-4', '4549-2', '17856-6']).build()
```

Now we can easily query observations with a view that uses the FHIRPath
`memberOf` function:

```py
# Creates the base observation view for convenience, typically done once per
# base type in a notebook.
obs = views.view_of('Observation')

# Create our HbA1c view based on the based observation view.
hba1c_obs = (
    obs.select({
         'id': obs.id,
         'patientId': obs.subject.idFor('Patient'),
         'status': obs.status,
         'time': obs.issued
    }).where(obs.code.memberOf(hba1c_value_set)))
```

## Working with external value sets and terminology services

You can also work with value sets defined by external terminology services. To
do so, you must first create a terminology service client.

This example uses the UMLS terminology service from the NIH. In order access
this terminology service, you need to
[sign up here.](https://uts.nlm.nih.gov/uts/signup-login) You should then enter
the API key found [on your profile page](https://uts.nlm.nih.gov/uts/profile) in
the place of 'your-umls-api-key' below.

```py
from google.fhir.r4.terminology import terminology_service_client

tx_client = terminology_service_client.TerminologyServiceClient({
    'http://cts.nlm.nih.gov/fhir/': ('apikey', 'your-umls-api-key')),
})
```

Before making queries against an externally-defined value set, you must first
get the codes defined by the value set and write them to a BigQuery table. You
only need to perform this step once. After doing so, you'll be able to reference
the value set definitions you've written in future queries.

```py
injury_value_set_url = 'http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.113762.1.4.1029.5'
wound_disorder_value_set_url = 'http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.113762.1.4.1219.178'
runner.materialize_value_set_expansion((injury_value_set_url, wound_disorder_value_set_url), tx_client)
```

To make queries against an externally-defined value set which you've saved to
BigQuery, you can simply refer to its URL.

```py
injury_conds =  cond.select({
    'id': cond.id,
    'patientId': cond.subject.idFor('Patient'),
    'codes': cond.code}
    ).where(cond.code.memberOf(injury_value_set_url))

runner.create_bigquery_view(injury_conds, 'injury_conditions')
```

## Saving FHIR Views as BigQuery Views

While `runner.to_dataframe` is convenient to retrieve data for local analysis,
it's often useful to create such flattened views in BigQuery itself. They can be
easily queried with much simpler SQL, or used by a variety of business
intelligence or other data analysis tools.

For this reason, the BigQueryRunner offers a `create_bigquery_view` method that
will convert the view definition into a
[BigQuery View](https://cloud.google.com/bigquery/docs/views), which can then
just be consumed as if it was a first-class table that is updated when the
underlying data is updated. Here's an example:

```py
runner.create_bigquery_view(hba1c_obs, 'hba1c_observations')
```

By default the view is created in the fhir_dataset used by the runner, but this
isn't always desirable (for example, a user may want to do their analysis in
their own, isolated dataset). Therefore it's common to specify a `view_dataset`
when creating the runner as the target for any views created. Here's an example:

```py
runner = bigquery_runner.BigQueryRunner(
    client,
    fhir_dataset='hcls-testing-data.fhir_20k_patients_analytics',
    view_dataset='example_project.diabetic_care_example')
```
