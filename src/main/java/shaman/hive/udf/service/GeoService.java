package shaman.hive.udf.service;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;

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
            return reader.country(InetAddress.getByName(ip)).getCountry().getIsoCode();
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
            return reader.country(InetAddress.getByName(ip)).getCountry().getName();
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
            return reader.cityIspOrg(InetAddress.getByName(ip)).getTraits().getIsp();
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
