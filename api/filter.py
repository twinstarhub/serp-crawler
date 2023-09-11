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

def facebook_filter(url):
    # Parse the URL

    # Old Format (with "people"):
    # https://www.facebook.com/people/Username/ProfileID/

    # New Format (without "people"):
    # https://www.facebook.com/Username/

    parsed_url = urlparse(url)

    # Separating the parts
    scheme = parsed_url.scheme  # https
    netloc = parsed_url.netloc  # facebook.com
    path = parsed_url.path      # /MichaelBagford/status/1397521783243382785

    # Split the path into a list
    path_components = path.split('/')
    if len(path_components) == 2 or path_components[1] == "people" :
        return True
    else:
        return False

def tiktok_filter(url):
    # Parse the URL
    # site:tiktok.com @michael bage
    parsed_url = urlparse(url)
    # Separating the parts
    scheme = parsed_url.scheme  # https
    netloc = parsed_url.netloc  # twitter.com
    path = parsed_url.path      # /MichaelBagford/status/1397521783243382785

    # Split the path into a list
    path_components = path.split('/')
    if len(path_components) == 2 and '@' in path_components[1]:
        return True
    else:
        return False
    
def reddit_filter(url):
    # Parse the URL
    # site:tiktok.com @michael bage
    parsed_url = urlparse(url)
    # Separating the parts
    path = parsed_url.path      # /MichaelBagford/status/1397521783243382785

    # Split the path into a list
    path_components = path.split('/')
    if len(path_components) == 3 and path_components[1]=='user':
        return True
    else:
        return False

def pinterest_filter(url):
    # Parse the URL
    # Platform list :
    #   Facebook, Twitter
    #     
    parsed_url = urlparse(url)

    # Separating the parts
    scheme = parsed_url.scheme  # https
    netloc = parsed_url.netloc  # twitter.com
    path = parsed_url.path      # /MichaelBagford/status/1397521783243382785
    # Split the path into a list
    path_components = path.split('/')
    if netloc == "about.pinterest.com" or netloc == "help.pinterest.com":
        return False

    if len(path_components) == 2 or (len(path_components) == 3 and path_components[2]==''):
        return True
    return False

def general_filter(url):
    # Parse the URL
    # Platform list :
    #   Facebook, Twitter
    #     
    parsed_url = urlparse(url)

    # Separating the parts
    scheme = parsed_url.scheme  # https
    netloc = parsed_url.netloc  # twitter.com
    path = parsed_url.path      # /MichaelBagford/status/1397521783243382785
    # Split the path into a list
    path_components = path.split('/')

    if len(path_components) == 2 or (len(path_components) == 3 and path_components[2]==''):
        return True
    else:
        return False

def linkedin_filter(url):
    if 'linkedin.com/in' in url:
        return True
    else:
        return False

# Create a dictionary to map platform names to filter functions
platform_filters = {
    'twitter': twitter_filter,
    'facebook': facebook_filter,
    'linkedin': linkedin_filter,
    'tiktok': tiktok_filter,
    'instagram':general_filter,
    'pinterest':pinterest_filter,
    'reddit':general_filter
}

def specialized_filter(url,platform_name):
    filter_function = platform_filters[platform_name]
    result = filter_function(url)
    return result


    