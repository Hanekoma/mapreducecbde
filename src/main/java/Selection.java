// Standard classes

import java.io.IOException;
// HBase classes
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Result;
// Hadoop classes
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Selection extends Configured implements Tool {

    private static String inputTable;
    private static String outputTable;

    private static final String VALUE_ATTRIBUTE = "valueatt";
    private static final String COLUMN_ATTRIBUTE = "columnatt";
    private static final String FAMILY_ATTRIBUTE = "familyatt";


       /*
            Explanation: This MapReduce job either requires at input both family and column name defined (family:column),
			or if only the column name is provided, it assumes that both family and column name are the same.
            For example, for a table with columns a, b and c, it would assume three families (a, b and c).

            This MapReduce takes three parameters:
            - Input HBase table from where to read data.
            - Output HBase table where to store data.
            - A [family:column] to select from.
            - The value which the column's value will be compared (=) to.

			We distinguish two following cases:
			1) 	For example, assume the following HBase table UsernameInput (the corresponding shell create statement follows):
				create 'UsernameInput', 'a' -- It contains one family: a
				put 'UsernameInput', 'key1', 'a:a', '1' -- It creates an attribute a under the a family with value 1
				put 'UsernameInput', 'key2', 'a:a', '2' -- It creates an attribute b under the b family with value 2

				A correct call would be this: yarn jar myJarFile.jar Selection UsernameInput out [a:]a 1
				It selects a if its value is 1
				The result (stored in UsernameOutput) would be: 'key1', 'a:a', '1' -- key2 is not there
				Notice that in this case providing family name is optional.

			2) 	However, assume the following case where HBase table is created as follows:
				create 'UsernameInputF', 'cf1' -- It contains one family: cf1
				put 'UsernameInputF', 'key1', 'cf1:a', '1' -- It creates an attribute a under the cf1 family with value 1
				put 'UsernameInputF', 'key2', 'cf1:a', '2' -- It creates an attribute a under the cf1 family with value 2

				In this case, a correct call would require both family and column defined, as follows:
				yarn jar myJarFile.jar Selection UsernameInputF UsernameOutputF cf1:a 1 -- It selects cf1:a when a=1
				The result (stored in UsernameOutputF) would be: 'key1', 'cf1:a', '1' -- key2 is not there
				Notice that in this case providing family name is mandatory.
       */

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Parameters missing: 'inputTable outputTable [family:]attribute value'");
            System.exit(1);
        }
        inputTable = args[0];
        outputTable = args[1];

        int tablesRight = checkIOTables(args);
        if (tablesRight == 0) {
            int ret = ToolRunner.run(new Selection(), args);
            System.exit(ret);
        } else {
            System.exit(tablesRight);
        }
    }


    //============================================================== checkTables
    private static int checkIOTables(String[] args) throws Exception {
        // Obtain HBase's configuration
        Configuration config = HBaseConfiguration.create();
        // Create an HBase administrator
        HBaseAdmin hba = new HBaseAdmin(config);

        // With an HBase administrator we check if the input table exists
        if (!hba.tableExists(inputTable)) {
            System.err.println("Input table does not exist");
            return 2;
        }
        // Check if the output table exists
        if (hba.tableExists(outputTable)) {
            System.err.println("Output table already exists");
            return 3;
        }

        // Create the columns of the output table
        HTableDescriptor htdInput = hba.getTableDescriptor(inputTable.getBytes());
        HTableDescriptor htdOutput = new HTableDescriptor(outputTable.getBytes());
        //Add columns to the new table
        //Optimization: if the input table does not have the asked family, the result should be empty and we can stop here
        String searchedFamily = args[2].split(":")[0];
        boolean found = false;
        for (byte[] key : htdInput.getFamiliesKeys()) {
            String currentFamily = new String(key);
            System.out.println("family = " + currentFamily);
            htdOutput.addFamily(new HColumnDescriptor(key));
            if (!found && currentFamily.equals(searchedFamily)) {
                found = true;
            }
        }

        //Create the new output table based on the descriptor we have been configuring
        hba.createTable(htdOutput);

        return found ? 0 : 5;
    }

    //============================================================== Job config
    public int run(String[] args) throws Exception {
        //Create a new job to execute

        //Retrive the configuration
        Job job = new Job(HBaseConfiguration.create());
        //Set the MapReduce class
        job.setJarByClass(Selection.class);
        //Set the job name
        job.setJobName("Selection");
        // Set the [family:]column & value
        String[] familyColumn = args[2].split(":");
        String family = familyColumn[0];
        String column = familyColumn[familyColumn.length == 1 ? 0 : 1];
        job.getConfiguration().setStrings(VALUE_ATTRIBUTE, args[3]);
        job.getConfiguration().setStrings(FAMILY_ATTRIBUTE, family);
        job.getConfiguration().setStrings(COLUMN_ATTRIBUTE, column);
        //Create an scan object
        Scan scan = new Scan();
        scan.setAttribute("scan.attributes.table.name", Bytes.toBytes(inputTable));
        //Set the Map and Reduce function
        TableMapReduceUtil.initTableMapperJob(inputTable, scan, Mapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(outputTable, Reducer.class, job);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 4;
    }


    //=================================================================== Mapper
    public static class Mapper extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable rowMetadata, Result values, Context context) throws IOException, InterruptedException {

            String value = context.getConfiguration().getStrings(VALUE_ATTRIBUTE)[0];
            String family = context.getConfiguration().getStrings(FAMILY_ATTRIBUTE)[0];
            String column = context.getConfiguration().getStrings(COLUMN_ATTRIBUTE)[0];
            String rowId = new String(rowMetadata.get(), "US-ASCII");

            String columnValue = new String(values.getValue(family.getBytes(), column.getBytes()));
            if (value.equals(columnValue)) {
                StringBuilder tuple = new StringBuilder("");
                for (KeyValue keyValue : values.raw()) {
                    tuple
                            .append(new String(keyValue.getFamily()))
                            .append(":")
                            .append(new String(keyValue.getQualifier()))
                            .append(":")
                            .append(new String(keyValue.getValue()))
                            .append(";");
                }
                context.write(new Text(rowId), new Text(tuple.toString()));
            }
        }
    }

    //================================================================== Reducer
    public static class Reducer extends TableReducer<Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> inputList, Context context) throws IOException, InterruptedException {
            Text tuple = inputList.iterator().next();

            // Create a tuple for the output table
            Put put = new Put(key.getBytes());
            //Set the values for the columns
            String[] triplets = tuple.toString().split(";");
            //Start at 1 to ignore empty string caused by leading ;
            for (String triplet : triplets) {
                String[] split = triplet.split(":");
                put.add(split[0].getBytes(), split[1].getBytes(), split[2].getBytes());
            }
            // Put the tuple in the output table
            context.write(key, put);
        }

    }
}
