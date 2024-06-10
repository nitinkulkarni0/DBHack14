The dataset consists of two main tables from the bright_data_business_information_linkedin_listing catalog, focusing on LinkedIn data:

linked_in_people_profiles_datasets: This table contains detailed profiles of individuals on LinkedIn. Key columns include:

timestamp: The date of the data entry.
id: A unique identifier for the profile.
name: The name of the individual.
city, country_code: Location information.
current_company: Includes nested fields like company_id and name, indicating the current employer.
position: The individual's current job title.
experience, education, languages, certifications: Details about the individual's professional background, education, languages they speak, and certifications they hold.
url: The URL to the LinkedIn profile.
Other fields include posts, groups, people_also_viewed, educations_details, avatar, recommendations, providing a comprehensive view of the individual's LinkedIn presence.
linked_in_company_information_datasets: This table provides information about companies on LinkedIn. Key columns include:

timestamp: The date of the data entry.
id, company_id: Unique identifiers for the company.
name: The name of the company.
country_code, locations, formatted_locations: Location and headquarters information.
followers, employees_in_linkedin: Metrics on LinkedIn presence and employee count.
about, specialties, industries: Descriptive information about the company's operations and areas of expertise.
company_size, organization_type: Information on the size and type of organization.
website, founded: The company's website and year of founding.
Other fields include image, logo, similar, sphere, url, type, updates, slogan, affiliated, get_directions_url, offering a detailed profile of the company's online presence and characteristics.
These tables are designed to provide a rich dataset for analysis of professional profiles and company information on LinkedIn, useful for market research, recruitment, and competitive analysis.

Ask your question...


!pip install databricks-genai-inference
%restart_python

from databricks_genai_inference import ChatCompletion

# Only required when running this example outside of a Databricks Notebook
# export DATABRICKS_HOST="https://dbc-572950c6-e11a.cloud.databricks.com"
# export DATABRICKS_TOKEN=""

response = ChatCompletion.create(model="databricks-dbrx-instruct",
                                messages=[{"role": "system", "content": "You are a helpful assistant and ensure you always respond politely and always use the datasets we have given as a base reference."},
                                          {"role": "user","content": "Using the dataset bright_data_business_information_linkedin_listing.datasets.linked_in_people_profiles_dataset as a reference, explain me what it contains?"}],
                                max_tokens=128)
print(f"response.message:{response.message}")
