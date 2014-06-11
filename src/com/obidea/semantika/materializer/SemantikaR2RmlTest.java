package com.obidea.semantika.materializer;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.LinkedHashSet;
import java.util.Set;

import junit.framework.TestSuite;

import org.openrdf.model.Statement;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.ntriples.NTriplesParser;

import io.github.johardi.r2rml.testsuite.ManifestTest;
import io.github.johardi.r2rml.testsuite.R2RmlBaseTest;

import com.obidea.semantika.app.ApplicationFactory;
import com.obidea.semantika.app.ApplicationManager;
import com.obidea.semantika.app.Environment;

public class SemantikaR2RmlTest extends R2RmlBaseTest
{
   private RdfMaterializerEngine mMaterializerEngine;

   private static String sExportFile = "/tmp/output.n3";

   public SemantikaR2RmlTest(String testIri, String testId, String testTitle, String testOutput,
         String mappingFile, String sqlScriptFile, boolean hasExpectedOutput)
   {
      super(testIri, testId, testTitle, testOutput, mappingFile, sqlScriptFile, hasExpectedOutput);
   }

   public static TestSuite suite() throws Exception
   {
      return ManifestTest.suite(new R2RmlBaseTest.Factory()
      {
         @Override
         public R2RmlBaseTest createR2RmlTest(String testIri, String testId, String testTitle, String testOutput,
               String mappingFile, String sqlScriptFile, boolean hasExpectedOutput)
         {
            return new SemantikaR2RmlTest(testIri, testId, testTitle, testOutput, mappingFile, sqlScriptFile, hasExpectedOutput);
         }
      });
   }

   @Override
   protected void runProcessor() throws Exception
   {
      ApplicationManager appManager = new ApplicationFactory()
         .setName("r2rml-test")
         .addProperty(Environment.CONNECTION_URL, getJdbcUrl())
         .addProperty(Environment.CONNECTION_DRIVER, getJdbcDriver())
         .addProperty(Environment.CONNECTION_USERNAME, getDbUser())
         .addProperty(Environment.CONNECTION_PASSWORD, getDbPassword())
         .addMappingSource(getMappingFile(), false)
         .createApplicationManager();

      try {
         mMaterializerEngine = appManager.createMaterializerEngine().useNTriples();
         mMaterializerEngine.start();
         mMaterializerEngine.materialize(new File(sExportFile));
      }
      finally {
         mMaterializerEngine.stop();
      }
   }

   @Override
   protected Set<Statement> getActualGraph() throws Exception
   {
      return readExportFile();
   }

   private final Set<Statement> readExportFile() throws Exception
   {
      NTriplesParser parser = new NTriplesParser();
      parser.getParserConfig().addNonFatalError(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES);
      parser.getParserConfig().addNonFatalError(BasicParserSettings.VERIFY_DATATYPE_VALUES);
      parser.getParserConfig().addNonFatalError(BasicParserSettings.NORMALIZE_DATATYPE_VALUES);
      parser.setPreserveBNodeIDs(true);
      
      Set<Statement> result = new LinkedHashSet<Statement>();
      parser.setRDFHandler(new StatementCollector(result));
      
      InputStream in = new URL("file:" + sExportFile).openStream();
      try {
         parser.parse(in, sExportFile);
      }
      finally {
         in.close();
      }
      return result;
   }

   @Override
   protected String getJdbcDriver()
   {
      return "com.mysql.jdbc.Driver";
   }

   @Override
   protected String getJdbcUrl()
   {
      return "jdbc:mysql://localhost:3306/r2rml-test?allowMultiQueries=true&sessionVariables=sql_mode=ANSI_QUOTES";
   }

   @Override
   protected String getDbUser()
   {
      return "semantika";
   }

   @Override
   protected String getDbPassword()
   {
      return "semantika";
   }
}
