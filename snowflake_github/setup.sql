CREATE OR REPLACE API INTEGRATION 
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/FerAou')
  ENABLED = TRUE;
  show integrations  ; 

 

  CREATE OR REPLACE API INTEGRATION api_integration_skills
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/FerAou')
  ALLOWED_AUTHENTICATION_SECRETS = (my_git_secret)
  ENABLED = TRUE;
  
  show integrations ;
desc integrations API_INTEGRATION_SKILLS ;

  CREATE OR REPLACE SECRET my_git_secret
  TYPE = PASSWORD
  USERNAME = 'FerAou'
  PASSWORD = 'my_token';