import pandas as pd
import re
import ipaddress
from collections import defaultdict
import traceback
class TextExtractor:
    """Enhanced class for extracting structured data from unstructured text with improved pattern matching"""
    
    def __init__(self):
        # Enhanced field patterns with better regex and multi-language support
        self.field_patterns = {
            'technology_id': [
                r'tech(?:nology)?[_\s]*id[:\s=\->\|]*([^\n\r,;]+)',
                r'tech(?:nology)?[_\s]*type[:\s=\->\|]*([^\n\r,;]+)',
                r'connection[_\s]*type[:\s=\->\|]*([^\n\r,;]+)',
                r'service[_\s]*type[:\s=\->\|]*([^\n\r,;]+)',
                r'tecnologia[:\s=\->\|]*([^\n\r,;]+)',
                r'tipo[_\s]*tecnologia[:\s=\->\|]*([^\n\r,;]+)',
                r'tipo[_\s]*conexao[:\s=\->\|]*([^\n\r,;]+)',
                r'tech[:\s=\->\|]*([^\n\r,;]+)',
                r'id[_\s]*tech[_\s]*([^\n\r,;]+)',
                r'\b(fiber|fibra|adsl|vdsl|cable|ethernet|wireless|wifi)\b',
            ],
            'provider_id': [
                r'provider[_\s]*id[:\s=\->\|]*([^\n\r,;]+)',
                r'carrier[_\s]*id[:\s=\->\|]*([^\n\r,;]+)',
                r'operator[_\s]*id[:\s=\->\|]*([^\n\r,;]+)',
                r'isp[_\s]*id[:\s=\->\|]*([^\n\r,;]+)',
                r'provedor[:\s=\->\|]*([^\n\r,;]+)',
                r'fornecedor[:\s=\->\|]*([^\n\r,;]+)',
                r'operadora[:\s=\->\|]*([^\n\r,;]+)',
                r'prestador[:\s=\->\|]*([^\n\r,;]+)',
                r'id[_\s]*provider[:\s=\->\|]*([^\n\r,;]+)',
                r'id[_\s]*operadora[:\s=\->\|]*([^\n\r,;]+)',
            ],
            'pop': [
                r'\bpop[_\s]*(?:id|name|code)?[:\s=\->\|]*([A-Za-z0-9\-_]{2,})\b',
                r'point[_\s]*of[_\s]*presence[:\s=\->\|]*([^\n\r,;]+)',
                r'ponto[_\s]*presenca[:\s=\->\|]*([^\n\r,;]+)',
                r'local[_\s]*(?:code|codigo)[:\s=\->\|]*([^\n\r,;]+)',
                r'site[_\s]*(?:id|code)[:\s=\->\|]*([^\n\r,;]+)',
                r'datacenter[:\s=\->\|]*([^\n\r,;]+)',
                r'central[:\s=\->\|]*([^\n\r,;]+)',
            ],
            'gateway': [
                r'gateway[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'gw[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'default[_\s]*gateway[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'roteador[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'route[_\s]*(?:ip)?[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}).*?(?:gateway|gw|roteador)',
                r'(?:gateway|gw|roteador).*?([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
            ],
            'interface_1': [
                r'interface[_\s]*1[:\s=\->\|]*([^\n\r,;]+)',
                r'int[_\s]*1[:\s=\->\|]*([^\n\r,;]+)',
                r'intf[_\s]*1[:\s=\->\|]*([^\n\r,;]+)',
                r'porta[_\s]*1[:\s=\->\|]*([^\n\r,;]+)',
                r'if1[:\s=\->\|]*([^\n\r,;]+)',
                r'eth0[:\s=\->\|]*([^\n\r,;]+)',
            ],
            'interface_2': [
                r'interface[_\s]*2[:\s=\->\|]*([^\n\r,;]+)',
                r'int[_\s]*2[:\s=\->\|]*([^\n\r,;]+)',
                r'intf[_\s]*2[:\s=\->\|]*([^\n\r,;]+)',
                r'porta[_\s]*2[:\s=\->\|]*([^\n\r,;]+)',
                r'if2[:\s=\->\|]*([^\n\r,;]+)',
                r'eth1[:\s=\->\|]*([^\n\r,;]+)',
            ],
            'access_point': [
                r'access[_\s]*point[:\s=\->\|]*([^\n\r,;]+)',
                r'ap[_\s]*(?:id|name)?[:\s=\->\|]*([^\n\r,;]+)',
                r'ponto[_\s]*acesso[:\s=\->\|]*([^\n\r,;]+)',
                r'wifi[_\s]*ap[:\s=\->\|]*([^\n\r,;]+)',
                r'wireless[_\s]*ap[:\s=\->\|]*([^\n\r,;]+)',
            ],
            'cpe': [
                r'cpe[_\s]*(?:id|model|type)?[:\s=\->\|]*([^\n\r,;]+)',
                r'customer[_\s]*(?:premise|equipment)[:\s=\->\|]*([^\n\r,;]+)',
                r'equipamento[_\s]*(?:cliente|terminal)?[:\s=\->\|]*([^\n\r,;]+)',
                r'terminal[_\s]*cliente[:\s=\->\|]*([^\n\r,;]+)',
                r'device[_\s]*(?:model|type)?[:\s=\->\|]*([^\n\r,;]+)',
                r'equipment[:\s=\->\|]*([^\n\r,;]+)',
            ],
            'ip_management': [
                r'management[_\s]*ip[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'mgmt[_\s]*ip[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'ip[_\s]*mgmt[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'gerencia[_\s]*ip[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'ip[_\s]*gerencia[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'admin[_\s]*ip[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}).*?(?:mgmt|management|gerencia|admin)',
                r'(?:mgmt|management|gerencia|admin).*?([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
            ],
            'ip_telephony': [
                r'ip[_\s]*telephony[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'voip[_\s]*ip[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'ip[_\s]*voip[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'sip[_\s]*ip[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'telefonia[_\s]*ip[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}).*?(?:voip|sip|telephony|telefonia)',
                r'(?:voip|sip|telephony|telefonia).*?([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
            ],
            'ip_block': [
                r'ip[_\s]*block[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'subnet[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'network[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'bloco[_\s]*ip[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'rede[_\s]*ip[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'cliente[_\s]*ip[:\s=\->\|]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:/\d{1,2})?)',
                r'\b([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})\b',
            ],
            'vlan': [
                r'vlan[_\s]*(?:id)?[:\s=\->\|]*(\d+)',
                r'vl[:\s=\->\|]*(\d+)',
                r'vid[:\s=\->\|]*(\d+)',
                r'vlan\s+(\d+)',
                r'v(\d{1,4})',
                r'tag[_\s]*(\d+)',
                r'vlan[_\s]*tag[:\s=\->\|]*(\d+)',
            ],
            'login_pppoe': [
                r'login[_\s]*pppoe[:\s=\->\|]*([^\n\r,;\s]+)',
                r'pppoe[_\s]*login[:\s=\->\|]*([^\n\r,;\s]+)',
                r'pppoe[_\s]*user[:\s=\->\|]*([^\n\r,;\s]+)',
                r'usuario[_\s]*pppoe[:\s=\->\|]*([^\n\r,;\s]+)',
                r'user[_\s]*pppoe[:\s=\->\|]*([^\n\r,;\s]+)',
                r'pppoe[:\s=\->\|]*([^\n\r,;\s]+)',
            ],
            'asn': [
                r'asn[_\s]*cliente[:\s=\->\|]*(\d+)',
                r'asn[_\s]*giga[:\s=\->\|]*(\d+)',
                r'asn[_\s]*(?:number)?[:\s=\->\|]*(\d+)',
                r'as[_\s]*(?:number)?[:\s=\->\|]*(\d+)',
                r'as(\d+)',
                r'autonomous[_\s]*system[:\s=\->\|]*(\d+)',
                r'sistema[_\s]*autonomo[:\s=\->\|]*(\d+)',
            ],
            'prefixes': [
                r'prefixes[:\s=\->\|]*([^\n\r,;]+)',
                r'prefix[:\s=\->\|]*([^\n\r,;]+)',
                r'prefixo[:\s=\->\|]*([^\n\r,;]+)',
                r'route[_\s]*prefix[:\s=\->\|]*([^\n\r,;]+)',
                r'anuncio[:\s=\->\|]*([^\n\r,;]+)',
                r'announcement[:\s=\->\|]*([^\n\r,;]+)',
            ],
            'pon_port': [
                r'pon[_\s]*port[:\s=\->\|]*([^\n\r,;]+)',
                r'porta[_\s]*pon[:\s=\->\|]*([^\n\r,;]+)',
                r'port[_\s]*pon[:\s=\->\|]*([^\n\r,;]+)',
                r'gpon[_\s]*port[:\s=\->\|]*([^\n\r,;]+)',
                r'pon[:\s=\->\|]*([0-9/\-]+)',
                r'slot[_\s]*pon[:\s=\->\|]*([^\n\r,;]+)',
            ],
            'onu_id': [
                r'onu[_\s]*id[:\s=\->\|]*([^\n\r,;]+)',
                r'ont[_\s]*id[:\s=\->\|]*([^\n\r,;]+)',
                r'id[_\s]*onu[:\s=\->\|]*([^\n\r,;]+)',
                r'id[_\s]*ont[:\s=\->\|]*([^\n\r,;]+)',
                r'onu[:\s=\->\|]*([0-9A-Za-z\-_]+)',
                r'ont[:\s=\->\|]*([0-9A-Za-z\-_]+)',
            ],
            'model_onu': [
                r'model[_\s]*onu[:\s=\->\|]*([^\n\r,;]+)',
                r'modelo[_\s]*onu[:\s=\->\|]*([^\n\r,;]+)',
                r'onu[_\s]*model[:\s=\->\|]*([^\n\r,;]+)',
                r'ont[_\s]*model[:\s=\->\|]*([^\n\r,;]+)',
                r'model[:\s=\->\|]*([^\n\r,;]+).*?onu',
                r'modelo[:\s=\->\|]*([^\n\r,;]+).*?onu',
            ],
            'slot': [
                r'slot[:\s=\->\|]*([0-9/\-]+)',
                r'porta[:\s=\->\|]*([0-9/\-]+)',
                r'port[:\s=\->\|]*([0-9/\-]+)',
                r'bay[:\s=\->\|]*([0-9/\-]+)',
                r'shelf[:\s=\->\|]*([0-9/\-]+)',
            ],
            'serial': [
                r'serial[_\s]*(?:number|num|no)?[:\s=\->\|]*([A-Za-z0-9\-_]+)',
                r'sn[:\s=\->\|]*([A-Za-z0-9\-_]+)',
                r'serie[:\s=\->\|]*([A-Za-z0-9\-_]+)',
                r'numero[_\s]*serie[:\s=\->\|]*([A-Za-z0-9\-_]+)',
                r'sernum[:\s=\->\|]*([A-Za-z0-9\-_]+)',
                r's/n[:\s=\->\|]*([A-Za-z0-9\-_]+)',
            ],
            'partnerid': [
                r'partner[_\s]*id[:\s=\->\|]*([^\n\r,;]+)',
                r'parceiro[_\s]*id[:\s=\->\|]*([^\n\r,;]+)',
                r'id[_\s]*partner[:\s=\->\|]*([^\n\r,;]+)',
                r'id[_\s]*parceiro[:\s=\->\|]*([^\n\r,;]+)',
                r'business[_\s]*partner[:\s=\->\|]*([^\n\r,;]+)',
            ],
            'circuitpartner': [
                r'circuit[_\s]*partner[:\s=\->\|]*([^\n\r,;]+)',
                r'circuito[_\s]*parceiro[:\s=\->\|]*([^\n\r,;]+)',
                r'partner[_\s]*circuit[:\s=\->\|]*([^\n\r,;]+)',
                r'parceiro[_\s]*circuito[:\s=\->\|]*([^\n\r,;]+)',
                r'circuit[_\s]*id[:\s=\->\|]*([^\n\r,;]+)',
            ],
            'serial_code': [
                r'serial[_\s]*code[:\s=\->\|]*([A-Za-z0-9\-_]+)',
                r'codigo[_\s]*serie[:\s=\->\|]*([A-Za-z0-9\-_]+)',
                r'code[_\s]*serial[:\s=\->\|]*([A-Za-z0-9\-_]+)',
                r'service[_\s]*code[:\s=\->\|]*([A-Za-z0-9\-_]+)',
            ],
            'mac': [
                # Fixed MAC address patterns with proper escaping
                r'mac[_\s]*(?:address|addr)?[:\s=\->\|]*([0-9A-Fa-f]{2}[:\-][0-9A-Fa-f]{2}[:\-][0-9A-Fa-f]{2}[:\-][0-9A-Fa-f]{2}[:\-][0-9A-Fa-f]{2}[:\-][0-9A-Fa-f]{2})',
                r'mac[_\s]*(?:address|addr)?[:\s=\->\|]*([0-9A-Fa-f]{12})',
                r'mac[_\s]*(?:address|addr)?[:\s=\->\|]*([0-9A-Fa-f]{4}\.[0-9A-Fa-f]{4}\.[0-9A-Fa-f]{4})',
                r'endereco[_\s]*mac[:\s=\->\|]*([0-9A-Fa-f:]{17})',
                r'physical[_\s]*address[:\s=\->\|]*([0-9A-Fa-f:]{17})',
                r'ether[:\s=\->\|]*([0-9A-Fa-f:]{17})',
                r'\b([0-9A-Fa-f]{2}[:\-][0-9A-Fa-f]{2}[:\-][0-9A-Fa-f]{2}[:\-][0-9A-Fa-f]{2}[:\-][0-9A-Fa-f]{2}[:\-][0-9A-Fa-f]{2})\b',
                r'\b([0-9A-Fa-f]{4}\.[0-9A-Fa-f]{4}\.[0-9A-Fa-f]{4})\b',
            ],
            'wifi_ssid': [
                r'wifi[_\s]*ssid[:\s=\->\|]*([^\n\r,;]+)',
                r'ssid[:\s=\->\|]*([^\n\r,;]+)',
                r'rede[_\s]*wifi[:\s=\->\|]*([^\n\r,;]+)',
                r'network[_\s]*name[:\s=\->\|]*([^\n\r,;]+)',
                r'essid[:\s=\->\|]*([^\n\r,;]+)',
                r'wireless[_\s]*network[:\s=\->\|]*([^\n\r,;]+)',
            ],
            'wifi_passcode': [
                r'wifi[_\s]*pass(?:word|code)[:\s=\->\|]*([^\n\r,;]+)',
                r'senha[_\s]*wifi[:\s=\->\|]*([^\n\r,;]+)',
                r'password[_\s]*wifi[:\s=\->\|]*([^\n\r,;]+)',
                r'key[_\s]*wifi[:\s=\->\|]*([^\n\r,;]+)',
                r'psk[:\s=\->\|]*([^\n\r,;]+)',
                r'wpa[_\s]*key[:\s=\->\|]*([^\n\r,;]+)',
                r'network[_\s]*key[:\s=\->\|]*([^\n\r,;]+)',
            ],
            'pop_description': [
                r'pop[_\s]*description[:\s=\->\|]*([^\n\r,;]+)',
                r'descricao[_\s]*pop[:\s=\->\|]*([^\n\r,;]+)',
                r'desc[_\s]*pop[:\s=\->\|]*([^\n\r,;]+)',
                r'description[:\s=\->\|]*([^\n\r,;]+)',
                r'designador[:\s=\->\|]*([^\n\r,;]+)',
                r'local[_\s]*description[:\s=\->\|]*([^\n\r,;]+)',
                r'site[_\s]*description[:\s=\->\|]*([^\n\r,;]+)',
            ]
        }
        
        # Field priority for smart assignment
        self.field_priorities = {
            'ip_management': 10,
            'gateway': 9,
            'ip_telephony': 8,
            'ip_block': 7,
            'mac': 6,
            'vlan': 5,
            'asn': 4,
            'serial': 3,
            'cpe': 2,
            'pop': 1
        }
        
        # Common false positive patterns to avoid
        self.false_positive_patterns = [
            r'^\s*$',  # Empty
            r'^n/a$', r'^na$', r'^null$', r'^none$',  # Null values
            r'^-+$', r'^=+$', r'^\*+$',  # Separators
            r'^\d{1,2}$',  # Single/double digits
            r'^[a-zA-Z]$',  # Single letters
        ]
    
    def extract_common_patterns(self, text):
        """Enhanced common pattern extraction with better validation"""
        if not isinstance(text, str):
            return {}
        
        patterns = {}
        
        # Enhanced IP address extraction with validation
        ipv4_pattern = r'\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(?:/(?:3[0-2]|[12]?[0-9]))?\b'
        ips = re.findall(ipv4_pattern, text)
        
        # Validate IP addresses
        valid_ips = []
        for ip in ips:
            try:
                ip_addr = ip.split('/')[0]
                ipaddress.IPv4Address(ip_addr)
                if not (ip_addr.startswith('0.') or ip_addr.startswith('255.')):
                    valid_ips.append(ip)
            except ipaddress.AddressValueError:
                continue
        
        if valid_ips:
            patterns['found_ips'] = valid_ips
        
        # Enhanced MAC address extraction with fixed patterns
        mac_patterns = [
            r'\b[0-9A-Fa-f]{2}[:\-][0-9A-Fa-f]{2}[:\-][0-9A-Fa-f]{2}[:\-][0-9A-Fa-f]{2}[:\-][0-9A-Fa-f]{2}[:\-][0-9A-Fa-f]{2}\b',
            r'\b[0-9A-Fa-f]{4}\.[0-9A-Fa-f]{4}\.[0-9A-Fa-f]{4}\b',
            r'\b[0-9A-Fa-f]{12}\b'
        ]
        
        valid_macs = []
        for pattern in mac_patterns:
            try:
                macs = re.findall(pattern, text)
                for mac in macs:
                    if self._is_valid_mac(mac):
                        valid_macs.append(mac)
            except re.error:
                continue
        
        if valid_macs:
            patterns['found_macs'] = list(set(valid_macs))
        
        # Enhanced VLAN extraction
        vlan_patterns = [
            r'vlan[_\s]*(?:id)?[:\s=]*(\d+)',
            r'vl[:\s=]*(\d+)',
            r'v(\d{1,4})',
            r'tag[_\s]*(\d+)'
        ]
        
        valid_vlans = []
        for pattern in vlan_patterns:
            try:
                vlans = re.findall(pattern, text, re.IGNORECASE)
                for vlan in vlans:
                    vlan_num = int(vlan)
                    if 1 <= vlan_num <= 4094:
                        valid_vlans.append(vlan_num)
            except (re.error, ValueError):
                continue
        
        if valid_vlans:
            patterns['found_vlans'] = list(set(valid_vlans))
        
        # Enhanced ASN extraction
        asn_patterns = [
            r'asn[_\s]*(?:cliente|giga)?[:\s]*(\d+)',
            r'as[_\s]*(\d+)',
            r'autonomous[_\s]*system[:\s]*(\d+)'
        ]
        
        valid_asns = []
        for pattern in asn_patterns:
            try:
                asns = re.findall(pattern, text, re.IGNORECASE)
                for asn in asns:
                    asn_num = int(asn)
                    if 1 <= asn_num <= 4294967295:
                        valid_asns.append(asn_num)
            except (re.error, ValueError):
                continue
        
        if valid_asns:
            patterns['found_asns'] = list(set(valid_asns))
        
        return patterns
    
    def _is_valid_mac(self, mac):
        """Validate MAC address format and content"""
        # Remove separators and check length
        mac_clean = re.sub(r'[:\-\.]', '', mac)
        
        if len(mac_clean) != 12:
            return False
        
        # Check if all characters are hex
        if not re.match(r'^[0-9A-Fa-f]{12}', mac_clean):
            return False
        
        # Avoid common invalid MACs
        invalid_macs = [
            '000000000000', 'ffffffffffff', '123456789abc',
            'aaaaaaaaaaaa', 'bbbbbbbbbbbb', 'cccccccccccc'
        ]
        
        if mac_clean.lower() in invalid_macs:
            return False
        
        return True
    
    def extract_using_patterns(self, text):
        """Enhanced pattern-based extraction with better validation"""
        if not isinstance(text, str):
            return {}
        
        extracted = {}
        confidence_scores = defaultdict(list)
        
        for field, patterns in self.field_patterns.items():
            field_candidates = []
            
            for pattern in patterns:
                try:
                    matches = re.findall(pattern, text, re.IGNORECASE)
                    for match in matches:
                        clean_match = self._clean_match(match, field)
                        if clean_match and self._validate_field_value(clean_match, field):
                            confidence = self._calculate_confidence(pattern, match, text, field)
                            field_candidates.append((clean_match, confidence))
                except re.error:
                    continue
            
            # Select best candidate based on confidence
            if field_candidates:
                field_candidates.sort(key=lambda x: x[1], reverse=True)
                best_match, best_confidence = field_candidates[0]
                
                # Only accept if confidence is above threshold
                if best_confidence > 0.3:
                    extracted[field] = best_match
                    confidence_scores[field] = best_confidence
        
        return extracted
    
    def _clean_match(self, match, field):
        """Clean and standardize extracted matches"""
        if not match:
            return None
        
        clean_match = str(match).strip()
        
        # Remove common prefixes/suffixes
        clean_match = re.sub(r'^[:\-=>\|]+\s*', '', clean_match)
        clean_match = re.sub(r'\s*[:\-=<\|]+', '', clean_match)
        
        # Field-specific cleaning
        if field in ['ip_management', 'ip_telephony', 'ip_block', 'gateway']:
            # Keep only IP address part
            ip_match = re.search(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?:/[0-9]{1,2})?\b', clean_match)
            if ip_match:
                clean_match = ip_match.group()
        
        elif field == 'mac':
            # Standardize MAC format
            clean_match = re.sub(r'[^0-9A-Fa-f:\-\.]', '', clean_match)
        
        elif field == 'vlan':
            # Extract numeric part only
            vlan_match = re.search(r'\b(\d+)\b', clean_match)
            if vlan_match:
                clean_match = vlan_match.group(1)
        
        elif field == 'asn':
            # Extract numeric part only
            asn_match = re.search(r'\b(\d+)\b', clean_match)
            if asn_match:
                clean_match = asn_match.group(1)
        
        elif field in ['serial', 'serial_code']:
            # Remove spaces and standardize
            clean_match = re.sub(r'\s+', '', clean_match)
        
        return clean_match.strip() if clean_match else None
    
    def _validate_field_value(self, value, field):
        """Validate extracted field values"""
        if not value:
            return False
        
        # Check against false positive patterns
        for pattern in self.false_positive_patterns:
            if re.match(pattern, value, re.IGNORECASE):
                return False
        
        # Field-specific validation
        if field in ['ip_management', 'ip_telephony', 'ip_block', 'gateway']:
            try:
                ip_addr = value.split('/')[0]  # Remove CIDR if present
                ipaddress.IPv4Address(ip_addr)
                return not (ip_addr.startswith('0.') or ip_addr.startswith('255.'))
            except ipaddress.AddressValueError:
                return False
        
        elif field == 'mac':
            return self._is_valid_mac(value)
        
        elif field == 'vlan':
            try:
                vlan_num = int(value)
                return 1 <= vlan_num <= 4094
            except ValueError:
                return False
        
        elif field == 'asn':
            try:
                asn_num = int(value)
                return 1 <= asn_num <= 4294967295
            except ValueError:
                return False
        
        elif field in ['serial', 'serial_code']:
            # Serial should be alphanumeric, at least 3 chars, not all digits
            return (len(value) >= 3 and 
                    re.match(r'^[A-Za-z0-9\-_]+', value) and 
                    not value.isdigit())
        
        elif field == 'pop':
            # POP should be reasonable length and alphanumeric
            return (2 <= len(value) <= 20 and 
                    re.match(r'^[A-Za-z0-9\-_]+', value))
        
        # Default validation for other fields
        return len(value) >= 2 and len(value) <= 100
    
    def _calculate_confidence(self, pattern, match, text, field):
        """Calculate confidence score for an extraction"""
        confidence = 0.5  # Base confidence
        
        # Pattern specificity bonus
        if field and pattern and field in pattern.lower():
            confidence += 0.2
        
        # Context validation - handle None values properly
        try:
            if text and match:
                match_str = str(match) if match is not None else ""
                text_str = str(text) if text is not None else ""
                
                match_pos = text_str.lower().find(match_str.lower())
                if match_pos >= 0:
                    # Check surrounding context (±50 chars)
                    start = max(0, match_pos - 50)
                    end = min(len(text_str), match_pos + len(match_str) + 50)
                    context = text_str[start:end].lower()
                    
                    # Field-specific context bonuses
                    if field == 'ip_management' and any(term in context for term in ['mgmt', 'management', 'gerencia', 'admin']):
                        confidence += 0.2
                    elif field == 'gateway' and any(term in context for term in ['gateway', 'gw', 'roteador', 'default']):
                        confidence += 0.2
                    elif field == 'mac' and any(term in context for term in ['mac', 'address', 'endereco']):
                        confidence += 0.2
                    elif field == 'vlan' and any(term in context for term in ['vlan', 'tag', 'vid']):
                        confidence += 0.2
        except (AttributeError, TypeError, ValueError):
            pass
        
        # Length penalty for very short matches
        try:
            if match and len(str(match)) < 3:
                confidence -= 0.2
        except (TypeError, AttributeError):
            pass
        
        # Bonus for validated formats
        if field in ['ip_management', 'ip_telephony', 'ip_block', 'gateway']:
            try:
                if match:
                    ipaddress.IPv4Address(str(match).split('/')[0])
                    confidence += 0.1
            except:
                pass
        
        return min(1.0, max(0.0, confidence))
    
    def smart_assignment(self, text, extracted_data):
        """Enhanced smart assignment with conflict resolution"""
        common_patterns = self.extract_common_patterns(text) or {}  # ✅ Ensure it's always a dict

        # Intelligent IP assignment based on context and order
        if 'found_ips' in common_patterns:
            ips = common_patterns['found_ips']
            ip_assignments = self._categorize_ips_by_context(text, ips)

            for field, ip in ip_assignments.items():
                if field not in extracted_data or not extracted_data[field]:
                    extracted_data[field] = ip

        # Auto-assign MAC if not already found
        if 'found_macs' in common_patterns and 'mac' not in extracted_data:
            extracted_data['mac'] = common_patterns['found_macs'][0]

        # Auto-assign VLAN with preference for higher numbers
        if 'found_vlans' in common_patterns and 'vlan' not in extracted_data:
            vlans = sorted(common_patterns['found_vlans'], reverse=True)
            extracted_data['vlan'] = vlans[0]

        # Auto-assign ASN
        if 'found_asns' in common_patterns and 'asn' not in extracted_data:
            extracted_data['asn'] = common_patterns['found_asns'][0]

        return extracted_data
        
    def _categorize_ips_by_context(self, text, ips):
        """Categorize IP addresses based on surrounding context"""
        ip_assignments = {}
        
        # Handle None or empty inputs
        if not text or not ips:
            return ip_assignments
            
        for ip in ips:
            if not ip:  # Skip None or empty IPs
                continue
                
            try:
                text_str = str(text) if text is not None else ""
                ip_str = str(ip) if ip is not None else ""
                
                ip_pos = text_str.lower().find(ip_str.lower())
                if ip_pos >= 0:
                    # Get context around IP (±100 chars)
                    start = max(0, ip_pos - 100)
                    end = min(len(text_str), ip_pos + len(ip_str) + 100)
                    context = text_str[start:end].lower()
                    
                    # Categorize based on context keywords
                    if any(term in context for term in ['mgmt', 'management', 'gerencia', 'admin']) and 'ip_management' not in ip_assignments:
                        ip_assignments['ip_management'] = ip
                    elif any(term in context for term in ['gateway', 'gw', 'roteador', 'default']) and 'gateway' not in ip_assignments:
                        ip_assignments['gateway'] = ip
                    elif any(term in context for term in ['voip', 'sip', 'telephony', 'telefonia']) and 'ip_telephony' not in ip_assignments:
                        ip_assignments['ip_telephony'] = ip
                    elif any(term in context for term in ['block', 'bloco', 'subnet', 'network', 'cliente']) and 'ip_block' not in ip_assignments:
                        ip_assignments['ip_block'] = ip
                    elif '/' in ip_str and 'ip_block' not in ip_assignments:  # CIDR notation suggests network block
                        ip_assignments['ip_block'] = ip
            except (AttributeError, TypeError, ValueError):
                continue
        
        # Fallback assignment for remaining IPs
        try:
            remaining_ips = [ip for ip in ips if ip and ip not in ip_assignments.values()]
            priority_fields = ['ip_management', 'gateway', 'ip_telephony', 'ip_block']
            
            for i, ip in enumerate(remaining_ips):
                if i < len(priority_fields) and priority_fields[i] not in ip_assignments:
                    ip_assignments[priority_fields[i]] = ip
        except (TypeError, AttributeError):
            pass
        
        return ip_assignments
    
    def process_text(self, text):
        """Enhanced main text processing function"""
        # Initialize result with all fields
        result = {field: None for field in self.field_patterns.keys()}
        
        if pd.isna(text) or not isinstance(text, str):
            return result
        
        try:
            # Extract using patterns
            extracted = self.extract_using_patterns(text)
            
            # Apply smart assignment
            extracted = self.smart_assignment(text, extracted)
            
            # Update result
            result.update(extracted)
            
        except Exception as e:
            # Log error but return empty result instead of crashing
            print(f"Text processing error: {str(e)}")
            traceback.print_exc()
        return result
    
    def get_field_list(self):
        """Return list of all extractable fields"""
        return list(self.field_patterns.keys())
    
    def get_extraction_stats(self, text):
        """Get detailed extraction statistics"""
        if not isinstance(text, str):
            return {}
        
        stats = {
            'text_length': len(text),
            'lines_count': len(text.split('\n')),
            'common_patterns': self.extract_common_patterns(text),
            'field_matches': {}
        }
        
        # Count potential matches per field
        for field, patterns in self.field_patterns.items():
            matches = 0
            for pattern in patterns:
                try:
                    matches += len(re.findall(pattern, text, re.IGNORECASE))
                except re.error:
                    continue
            stats['field_matches'][field] = matches
        
        return stats