package shaman.hive.udf.service;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityIspOrgResponse;
import com.maxmind.geoip2.model.CountryResponse;

public class GeoService {
    
    private static class ResourceHolder {
        private static final GeoService geoService = new GeoService();
    }
   
    private DatabaseReader reader;
    
    private GeoService() {
        try {
            reader = new DatabaseReader.Builder(new File("GeoLite2-City.mmdb")).build();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    
    public static GeoService getInstance() {
        return ResourceHolder.geoService;
    }
    
    public String getCountryCode(String ip) {
        try {
            CountryResponse cr = reader.country(InetAddress.getByName(ip));
            return cr.getCountry().getIsoCode();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (GeoIp2Exception e) {
            e.printStackTrace();
        }
        return "--";
    }
    
    public String getCountryName(String ip) {
        try {
            CountryResponse cr = reader.country(InetAddress.getByName(ip));
            return cr.getCountry().getName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (GeoIp2Exception e) {
            e.printStackTrace();
        }
        return "--";
    }
    
    public String getIsp(String ip) {
        try {
            CityIspOrgResponse ci = reader.cityIspOrg(InetAddress.getByName(ip));
            return ci.getTraits().getIsp();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (GeoIp2Exception e) {
            e.printStackTrace();
        }
        return "--";
    }
}
