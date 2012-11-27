package com.splout.db.integration;

/*
 * #%L
 * Splout SQL Hadoop library
 * %%
 * Copyright (C) 2012 Datasalt Systems S.L.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.datasalt.pangool.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.mortbay.log.Log;

import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat;
import com.datasalt.pangool.utils.HadoopUtils;
import com.splout.db.common.PartitionMap;
import com.splout.db.common.ReplicationMap;
import com.splout.db.common.SploutClient;
import com.splout.db.hadoop.TablespaceSpec;
import com.splout.db.hadoop.TablespaceGenerator;
import com.splout.db.hadoop.TupleSampler;
import com.splout.db.hadoop.TupleSampler.SamplingType;

/**
 * Demo based on hypothetical retail data (payments, etc). Use the main() method for running it.
 */
public class RetailDemo {

	final static int N_TIENDAS = 100;
	final static int N_CLIENTES = 1000;
	final static int N_PRODUCTOS = 500;
	final static int N_PRODUCTOS_PER_TICKET = 5;
	final static double MAX_PRECIO = 100.0;
	final static int DAY_SPAN = 365;
	final static DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd");

	public void generate(long nRegs, String dnodes, String qnode, Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();

		FileSystem fS = FileSystem.get(conf);
		HadoopUtils.deleteIfExists(fS, inputPath);
		HadoopUtils.deleteIfExists(fS, outputPath);

		NullWritable nullValue = NullWritable.get();
		Schema retailSchema = new Schema("retail",
		    Fields.parse("tienda:string, cliente:int, ticket:double, producto:int, precio:double, fecha:string"));
		ITuple tuple = new Tuple(retailSchema);

		TupleFile.Writer writer = new TupleFile.Writer(fS, conf, inputPath, retailSchema);

		// Writes nRegs Tuples to HDFS
		long soFar = 0;

		while(soFar < nRegs) {
			int tienda = (int) (Math.random() * N_TIENDAS);
			int cliente = (int) (Math.random() * N_CLIENTES);

			tuple.set("tienda", "T" + tienda);
			tuple.set("cliente", cliente);
			double[] precios = new double[N_PRODUCTOS_PER_TICKET];
			double ticket = 0;
			for(int i = 0; i < N_PRODUCTOS_PER_TICKET; i++) {
				precios[i] = ((int) (Math.random() * MAX_PRECIO * 100)) / 100;
				precios[i] = Math.max(precios[i], 5.00);
				ticket += precios[i];
			}
			tuple.set("ticket", ticket);
			long fecha = System.currentTimeMillis() - ((long) (Math.random() * DAY_SPAN * 24 * 60 * 60 * 1000));
			tuple.set("fecha", fmt.print(fecha));
			for(int i = 0; i < N_PRODUCTOS_PER_TICKET; i++) {
				int producto = (int) (Math.random() * N_PRODUCTOS);
				tuple.set("precio", precios[i]);
				tuple.set("producto", producto);
				writer.append(tuple);
				soFar++;
			}
		}
		writer.close();

		// Generate Splout view (cliente)
		String[] dnodeArray = dnodes.split(",");
		TablespaceSpec tablespace = TablespaceSpec.of(retailSchema, "cliente", inputPath, new TupleInputFormat(), dnodeArray.length);
		TablespaceGenerator generateView = new TablespaceGenerator(tablespace, outputPath);
		generateView.generateView(conf, SamplingType.DEFAULT, new TupleSampler.DefaultSamplingOptions());
		PartitionMap partitionMap = generateView.getPartitionMap();
		ReplicationMap replicationMap = ReplicationMap.oneToOneMap(dnodeArray);

		Path deployUri = new Path(outputPath, "store").makeQualified(fS);

		SploutClient client = new SploutClient(qnode);
		client.deploy("retailcliente", partitionMap, replicationMap, deployUri.toUri());

		// Generate Splout view (tienda)
		Path output2 = new Path(outputPath + "-2");
		HadoopUtils.deleteIfExists(fS, output2);
		tablespace = TablespaceSpec.of(retailSchema, "tienda", inputPath, new TupleInputFormat(), dnodeArray.length);
		generateView = new TablespaceGenerator(tablespace, output2);

		generateView.generateView(conf, SamplingType.DEFAULT, new TupleSampler.DefaultSamplingOptions());
		partitionMap = generateView.getPartitionMap();
		deployUri = new Path(output2, "store").makeQualified(fS);
		client.deploy("retailtienda", partitionMap, replicationMap, deployUri.toUri());
	}

	public static void main(String[] args) throws Exception {
		if(args.length != 3) {
			System.err.println("Wrong arguments provided.\n\n");
			System.out
			    .println("Usage: [dNodes] [qNode] [nRegisters] \n\nExample: localhost:9002,localhost:9003 http://localhost:9001 100000 \n");
			System.exit(-1);
		}
		String dnodes = args[0];
		String qnode = args[1];
		long nRegisters = Long.parseLong(args[2]);
		RetailDemo generator = new RetailDemo();
		Log.info("Parameters: registers=" + nRegisters + " dnodes: [" + dnodes + "]");

		generator.generate(nRegisters, dnodes, qnode, new Path("in-generate"), new Path("out-generate"));
	}
}