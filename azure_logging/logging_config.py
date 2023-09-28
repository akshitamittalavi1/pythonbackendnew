from azure_security.azure_keyvault import AzureKeyvault

keyvault_obj = AzureKeyvault()
client = keyvault_obj.keyvault_client()


def fetch_secret(name):
    try:
        secret = str(client.get_secret(name))
        return secret
    except Exception as ex:
        return False

instrumentation_key = fetch_secret("jpstgeka-app-insights-access-key")
app_insights_conn_str = fetch_secret("jpstgeka-app-insights-conn-str")