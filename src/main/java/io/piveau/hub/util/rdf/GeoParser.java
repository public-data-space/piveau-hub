package io.piveau.hub.util.rdf;

import io.vertx.core.json.JsonObject;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.geojson.GeoJsonWriter;
import org.locationtech.jts.io.gml2.GMLReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;


import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;

public class GeoParser {

    public GeoParser() {
    }

    public JsonObject WKTtoGeoJSON(String input) throws GeoParsingException {
        WKTReader reader = new WKTReader();
        Geometry geometry;
        try {
            geometry = reader.read(input);
            GeoJsonWriter geoJSONWriter = new GeoJsonWriter();
            geoJSONWriter.setEncodeCRS(false);
            String gJson = geoJSONWriter.write(geometry);
            return new JsonObject(gJson);
        } catch (ParseException e) {
            throw new GeoParsingException();
        } catch (RuntimeException e) {
            throw new GeoParsingException(e.getMessage());
        }
    }

    public JsonObject GML2toGeoJSON(String input) throws GeoParsingException {
        GMLReader gmlReader = new GMLReader();
        try {
            Geometry geometry = gmlReader.read(input, null);
            GeoJsonWriter geoJSONWriter = new GeoJsonWriter();
            geoJSONWriter.setEncodeCRS(false);
            String gJson = geoJSONWriter.write(geometry);
            return new JsonObject(gJson);
        } catch (SAXException e) {
            throw new GeoParsingException(e.getMessage());
        } catch (IOException e) {
            throw new GeoParsingException(e.getMessage());
        } catch (ParserConfigurationException e) {
            throw new GeoParsingException(e.getMessage());
        }
    }

    public JsonObject GML3toGeoJSON(String input) throws GeoParsingException {
        DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder builder = domFactory.newDocumentBuilder();
            Document document = builder.parse(new InputSource((new StringReader(input))));
            XPath xPath = XPathFactory.newInstance().newXPath();
            String node1 = ((Node) xPath.evaluate("/Envelope/upperCorner/text()", document, XPathConstants.NODE)).getNodeValue();
            String node2 = ((Node) xPath.evaluate("/Envelope/lowerCorner/text()", document, XPathConstants.NODE)).getNodeValue();
            String[] node1Splitted = node1.split("\\s+");
            String[] node2Splitted = node2.split("\\s+");
            GeometryFactory fact = new GeometryFactory();
            /**
             * Latitude - Second Parameter - Y
             * Longitude - First Parameter - X
             */
            Envelope envelope = new Envelope(
                    new Coordinate(Double.valueOf(node2Splitted[1]), Double.valueOf(node2Splitted[0])),
                    new Coordinate(Double.valueOf(node1Splitted[1]), Double.valueOf(node1Splitted[0])));
            Geometry geometry = fact.toGeometry(envelope);
            //LOGGER.info(envelope.toString());
            GeoJsonWriter geoJSONWriter = new GeoJsonWriter();
            geoJSONWriter.setEncodeCRS(false);
            String gJson = geoJSONWriter.write(geometry);
            return new JsonObject(gJson);
        } catch (Exception e) {
            throw new GeoParsingException(e.getMessage());
        }
    }
}
