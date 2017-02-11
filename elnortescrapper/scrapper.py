"""
Scrapper

This module implements the Scrapper class, which is a web scrapper for real
estate classified ads of El Norte Avisos de Ocasión.
"""

import requests
from lxml import html
from bs4 import BeautifulSoup
from pandas import DataFrame
import random
import time
from datetime import datetime
from collections import OrderedDict
import re
import logging


class Scrapper():
    """Web scrapper for real estate classified ads of El Norte
    Avisos de Ocasión www.avisosdeocasion.com website.
    It works with the categories of houses for sale in states of Mexico.

    Example:
        >>> from reformascrapper import Scrapper
        >>> my_scrapper = Scrapper()
        >>> df = my_scrapper.scrap('venta_casas_cdmx', ad_limit=100)
        Started web scraping
        Progress: 100 scrapped ads
        Finished web scraping
        >>> df.columns
        Index(['precio', 'zona', 'colonia', 'visitas', 'plantas',
               'm2_terreno', 'recámaras', 'baños', 'm2_constr', 'fecha_pub',
               'latitude', 'longitude', 'url'],
              dtype='object')
        >>> df_venta_casas.shape
        (100, 13)
        >>> df[['precio','colonia', 'm2_constr']].head()
                                              precio               colonia  m2_constr
        timestamp                                                                    
        2017-02-05 23:24:28.235641  11,800,000 pesos              AILES II        361
        2017-02-05 23:24:32.084964   1,900,000 pesos  AMPLIACION MIGUEL HI        170
        2017-02-05 23:24:32.687735   6,175,000 pesos  AMPLIACION MIGUEL HI        295
        2017-02-05 23:24:33.266031   4,300,000 pesos  AMPLIACION MIGUEL HI        195
        2017-02-05 23:24:34.186730   3,420,000 pesos  AMPLIACION MIGUEL HI        299
        
        >>> my_scrapper.categories
                                   category
        0                  venta_casas_cdmx
        1            venta_casas_nuevo_leon
        2               venta_casas_jalisco
        3        venta_casas_aguascalientes
        4       venta_casas_baja_california
        5   venta_casas_baja_california_sur
        6              venta_casas_campeche
        7               venta_casas_chiapas
        8             venta_casas_chihuahua
        9              venta_casas_coahuila
        10               venta_casas_colima
        11              venta_casas_durango
        12               venta_casas_edomex
        13           venta_casas_guanajuato
        14             venta_casas_guerrero
        15              venta_casas_hidalgo
        16            venta_casas_michoacan
        17              venta_casas_morelos
        18              venta_casas_nayarit
        19               venta_casas_oaxaca
        20               venta_casas_puebla
        21            venta_casas_queretaro
        22         venta_casas_quintana_roo
        23             venta_casas_san_luis
        24              venta_casas_sinaloa
        25               venta_casas_sonora
        26              venta_casas_tabasco
        27           venta_casas_tamaulipas
        28             venta_casas_tlaxcala
        29             venta_casas_veracruz
        30              venta_casas_yucatan
        31                venta_casas_texas

    Todo:
        * Support categories venta_departamentos
        * Documentation
        * Demo in iPython Notebooks
        * Utility methods to clean the data (currency, publication date, etc.)
        * Add tests
    """

    def __init__(self, verbose=False):
        """Initialize the URLs of the ad categories and the verbose mode using
        logging package.
        """
        # OrderedDict with
        # key: the name of the category
        # value: the url of the category and the method that converts an ad of
        # that category into a DataFrame
        self._categories = OrderedDict([
            ('venta_casas_cdmx', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=1&Plaza=1&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_nuevo_leon', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=2&Plaza=2&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_jalisco', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=3&Plaza=3&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_aguascalientes', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=4&Plaza=4&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_baja_california', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=5&Plaza=5&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_baja_california_sur', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=6&Plaza=6&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_campeche', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=7&Plaza=7&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_chiapas', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=8&Plaza=8&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_chihuahua', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=9&Plaza=9&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_coahuila', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=10&Plaza=10&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_colima', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=11&Plaza=11&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_durango', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=12&Plaza=12&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_edomex', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=13&Plaza=13&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_guanajuato', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=14&Plaza=14&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_guerrero', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=15&Plaza=15&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_hidalgo', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=16&Plaza=16&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_michoacan', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=17&Plaza=17&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_morelos', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=18&Plaza=18&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_nayarit', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=19&Plaza=19&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_oaxaca', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=20&Plaza=20&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_puebla', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=21&Plaza=21&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_queretaro', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=22&Plaza=22&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_quintana_roo', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=23&Plaza=23&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_san_luis', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=24&Plaza=24&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_sinaloa', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=25&Plaza=25&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_sonora', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=26&Plaza=26&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_tabasco', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=27&Plaza=27&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_tamaulipas', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=28&Plaza=28&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_tlaxcala', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=29&Plaza=29&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_veracruz', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=30&Plaza=30&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_yucatan', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=31&Plaza=31&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
            ('venta_casas_texas', ['http://www.avisosdeocasion.com/Resultados-Inmuebles.aspx?&PlazaBusqueda=34&Plaza=34&pagina=1&idinmueble=3', self._venta_casas_ad_to_df]),
        ])

        if verbose:
            logging.basicConfig(format='%(message)s', level=logging.DEBUG)
        else:
            logging.basicConfig(format='%(message)s', level=logging.WARNING)
            # Setting it to logging.INFO shows many messages from http conns

    def _request_helper(self, url):
        """This method makes a request to the server and there's a probability
        that it makes a pause afterwards. If the request timeouts, it retries
        the request multiple times with a longer pause in between.

        Returns:
            requests.models.Response
        """
        remaining_attempts = 10
        success = False
        while remaining_attempts and not success:
            try:
                response = requests.get(url)

                if random.randint(1, 10) == 1:
                    wait_time = random.randint(1, 5)
                    logging.debug("Waiting ", wait_time, " seconds")
                    time.sleep(wait_time)
            except:
                # Server denied request
                waiting_time = random.randint(5, 15)
                logging.debug("Server communication problem. Retrying in " +
                              str(waiting_time) + ' seconds')
                time.sleep(waiting_time)
                remaining_attempts -= 1
            else:
                success = True
        if not success:
            raise Exception("Server communication problem")

        return response

    def _venta_casas_ad_to_df(self, ad):
        """Uses regex to convert a string representation of an ad of category
        venta_casas into a 1-row DataFrame. Missing values of the ad are going
        to be set as None.

        Parameters:
            ad (str): String representation of an ad.

        Returns:
            pandas.core.frame.DataFrame: A 1-row DataFrame with the ad.
        """
        columns = ('precio', 'zona', 'colonia', 'visitas', 'plantas',
                   'm2_terreno', 'recámaras', 'baños', 'm2_constr',
                   'fecha_pub', 'latitude', 'longitude', 'url')
        # Create a dummy dictionary with the columns as keys and None as value
        dictionary = OrderedDict(zip([i for i in columns],
                                     [None for i in columns]))

        # Identify the elements of the ad and create a dictionary with them
        # Mind that some ads have missing elements in the website

        # precio
        regex_ans = re.search('\$([,0-9]+\s+\w*)', ad)
        if regex_ans:
            dictionary['precio'] = regex_ans.group(1)
        # zona
        regex_ans = re.search('ZONA:\s+([\w ]+)', ad)
        if regex_ans:
            dictionary['zona'] = regex_ans.group(1)
        # colonia
        regex_ans = re.search('COLONIA:\s+([\w ]+)', ad)
        if regex_ans:
            dictionary['colonia'] = regex_ans.group(1)
        # visitas
        regex_ans = re.search('([\d,]+)\s+visitas', ad)
        if regex_ans:
            dictionary['visitas'] = regex_ans.group(1)
        # plantas
        regex_ans = re.search('(\d+)\s+Planta', ad)
        if regex_ans:
            dictionary['plantas'] = regex_ans.group(1)
        # m2_terreno
        regex_ans = re.search('([\d.]+)m²\s+de\s+Terreno', ad)
        if regex_ans:
            dictionary['m2_terreno'] = regex_ans.group(1)
        # recámaras
        regex_ans = re.search('(\d+)\s+Recámara', ad)
        if regex_ans:
            dictionary['recámaras'] = regex_ans.group(1)
        # baños
        regex_ans = re.search('([\d.]+)\s+Baño', ad)
        if regex_ans:
            dictionary['baños'] = regex_ans.group(1)
        # m2_constr
        regex_ans = re.search('([\d.]+)m²\s+de\s+Construcción', ad)
        if regex_ans:
            dictionary['m2_constr'] = regex_ans.group(1)
        # fecha_pub
        regex_ans = re.search('Publicado[\s\w]+( \d+ de \w+)\n', ad)
        if regex_ans:
            dictionary['fecha_pub'] = regex_ans.group(1)
        # latitude
        regex_ans = re.search('latitude=([\d+-.]+)\n', ad)
        if regex_ans:
            dictionary['latitude'] = regex_ans.group(1)
        # longitude
        regex_ans = re.search('longitude=([\d+-.]+)\n', ad)
        if regex_ans:
            dictionary['longitude'] = regex_ans.group(1)
        # url
        regex_ans = re.search('(http.+)\n', ad)
        if regex_ans:
            dictionary['url'] = regex_ans.group(1)

        # Convert the dictionary to DataFrame
        df = DataFrame(dictionary, index=[None])
        df.index = [datetime.now().isoformat()]
        df.index.name = 'timestamp'
        return df

    def _venta_departamentos_ad_to_df(self):
        pass

    def _venta_terrenos_ad_to_df(self):
        pass

    def _pages_of_ads(self, category, initial_page):
        """
        Args:
            category (str): The name of the category of ads.
            initial_page (int): Initial page of ads to start the scrap.

        Yields:
            requests.models.Response: The next page of ads of the given
                category.
        """
        page_number = initial_page
        while True:
            url_of_category = self._categories[category][0]
            url_on_page = re.sub('pagina=[\d]+',
                                 'pagina='+str(page_number), url_of_category)
            page_number += 1

            # Check if this is the last page of ads
            try:
                page = self._request_helper(url_on_page)
                tree = html.fromstring(page.content)
                last_page_message = tree.xpath(('//div[@id="celda_rut1"]'
                                               '/text()'))
                logging.debug('Fetched ' + url_on_page)
            except:
                raise Exception("Detection of last page of ads failed")
            else:
                if 'No se encontraron avisos' not in str(last_page_message):
                    yield page
                else:
                    # Stop iteration
                    return

    def _ads_in_a_page(self, page):
        """
        Args:
            page (requests.models.Response): the page from which the ads URLs
                are to be extracted and in turn visted and scrapped as string.

        Yields:
            str: A string representation of the next ad in the page.
        """
        page_soup = BeautifulSoup(page.text, 'html.parser')
        for td in page_soup.find_all('td', class_='ar12grisb'):
            if 'ar12gris' not in str(td.get('class')):
                continue
            for elem in td:
                # Use try-except because some td are empty
                try:
                    entry = elem.get_text()
                except:
                    pass
                else:
                    if '$' not in entry:
                        # This element in the td is not an ad, ommit it
                        continue

                    # Because the website intentionally lists some ads
                    # incomplete, it's needed to visit each ad to scrap it.

                    # Get the url of the ad
                    ad_url = elem.find_all('a')[1].get('href')

                    ad_page = self._request_helper(ad_url)
                    ad_soup = BeautifulSoup(ad_page.text, 'html.parser')
                    ad_as_string = ''

                    # url of the ad
                    ad_as_string += ad_url+'\n'
                    # visitas
                    visitas = ad_soup.find_all('div', id='pestanas')
                    ad_as_string += visitas[0].get_text()+'\n'
                    # zona, colonia and precio
                    header_zona_colonia_precio = ad_soup.find_all(
                        'div', id='highlights')
                    ad_as_string += header_zona_colonia_precio[0].get_text()
                    ad_as_string += '\n'
                    # zona and estado
                    table = ad_soup.find_all('table', class_='ar13gris')
                    info = table[0].find_all('tr')[0].get_text()
                    ad_as_string += info + '\n'
                    # square meters and more details
                    for td in ad_soup.find_all('td', class_='carac_td'):
                        ad_as_string += td.get_text()+'\n'
                    # geolocation
                    try:
                        for div in ad_soup.find_all('div', id='divMapa'):
                            div_content = str(div)
                            if 'LatitudGM' and 'LongitudGM' in div_content:
                                lat = re.search('LatitudGM=([\d+-.]+)',
                                                div_content).group(1)
                                longit = re.search('LongitudGM=([\d+-.]+)',
                                                   div_content).group(1)
                                ad_as_string += 'latitude=' + lat + '\n'
                                ad_as_string += 'longitude=' + longit + '\n'
                    except:
                        pass
                    yield ad_as_string

    def _ad_to_dataframe(self, ad_text, category):
        """Converts the ad of the given category to a 1-row DataFrame.

        Args:
            ad_text (str): A string representation of an ad.
            category (str): The name of the category of ads.

        Returns:
            pandas.core.frame.DataFrame
        """
        method_to_call = self._categories[category][1]
        df = method_to_call(ad_text)
        return df

    def scrap(self, category, ad_limit=None, initial_page=1):
        """Returns a DataFrame with the information of the ads of the given
        category.

        Args:
            category (str): Name of the category of ads.
            ad_limit (int): Number of ads to scrap.
            initial_page (int): Initial page of ads to start the scrap.

        Raises:
            Exception: The given ad category is not supported.

        Returns:
            pandas.core.frame.DataFrame: DataFrame with the scrapped ads.
        """
        if category not in self._categories.keys():
            raise Exception(('Unrecognized category. '
                             'Try the atribute \'categories\''))

        final_df = DataFrame()
        scrapped_ads = 0
        remaining_ads = True
        print("Started web scraping")
        for page in self._pages_of_ads(category, initial_page):
            if not remaining_ads:
                break
            for ad in self._ads_in_a_page(page):
                if not remaining_ads:
                    break
                single_ad = self._ad_to_dataframe(ad, category)
                final_df = final_df.append(single_ad)
                scrapped_ads += 1
                print("Progress: %s scrapped ads\r" % (str(scrapped_ads)),
                      end='')

                if ad_limit and scrapped_ads >= ad_limit:
                    remaining_ads = False
                    break
        print("\nFinished web scraping")
        return final_df

    @property
    def categories(self):
        """Returns a DataFrame with the supported categories to scrap ads.
        
        Returns:
            pandas.core.frame.DataFrame: DataFrame with the supported
                categories.
        """
        df = DataFrame({'category': list(self._categories.keys())})
        return df
