from urllib.parse import urlparse
def twitter_filter(url):
    # Parse the URL
    parsed_url = urlparse(url)

    # Separating the parts
    scheme = parsed_url.scheme  # https
    netloc = parsed_url.netloc  # twitter.com
    path = parsed_url.path      # /MichaelBagford/status/1397521783243382785

    # Split the path into a list
    path_components = path.split('/')
    if len(path_components) == 2:
        return True
    else:
        return False

def linkedin_filter(url):
    if 'linkedin.com/in' in url:
        return True
    else:
        return False

# def general_filter(platform_name):


    