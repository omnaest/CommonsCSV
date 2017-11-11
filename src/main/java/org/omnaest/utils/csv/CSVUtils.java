/*

	Copyright 2017 Danny Kunz

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.


*/
package org.omnaest.utils.csv;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.omnaest.utils.StreamUtils;

/**
 * Helper for reading and writing CSV files
 *
 * @author Omnaest
 */
public class CSVUtils
{
	/**
	 * Similar to {@link #parse(InputStream)} with a buffered {@link FileInputStream}
	 *
	 * @param file
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static Stream<Map<String, String>> parse(File file) throws FileNotFoundException, IOException
	{
		return parse(IOUtils.toBufferedInputStream(new FileInputStream(file)));
	}

	/**
	 * Similar to {@link #parse(String, Charset)} with {@link StandardCharsets#UTF_8}
	 *
	 * @param content
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static Stream<Map<String, String>> parse(String content) throws FileNotFoundException, IOException
	{
		return parse(content, StandardCharsets.UTF_8);
	}

	/**
	 * Similar to {@link #parse(InputStream)} the given {@link String} content with the given {@link Charset}
	 *
	 * @param content
	 * @param charset
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static Stream<Map<String, String>> parse(String content, Charset charset) throws FileNotFoundException, IOException
	{
		return parse(IOUtils.toInputStream(content, charset));
	}

	/**
	 * Similar to {@link #parse(InputStream, CSVFormat)} uses {@link CSVFormat#EXCEL} with headers
	 *
	 * @param inputStream
	 * @return
	 * @throws IOException
	 */
	public static Stream<Map<String, String>> parse(InputStream inputStream) throws IOException
	{
		return parse(inputStream, CSVFormat.EXCEL.withHeader());
	}

	/**
	 * Similar to {@link #parse(InputStream, CSVFormat, Charset)} using {@link StandardCharsets#UTF_8}
	 *
	 * @param inputStream
	 * @param csvFormat
	 * @return
	 * @throws IOException
	 */
	public static Stream<Map<String, String>> parse(InputStream inputStream, CSVFormat csvFormat) throws IOException
	{
		return parse(inputStream, csvFormat, StandardCharsets.UTF_8);
	}

	/**
	 * Similar to {@link #parse(InputStream, CSVFormat, Charset)}
	 * 
	 * @param file
	 * @param csvFormat
	 * @param charset
	 * @return
	 * @throws IOException
	 */
	public static Stream<Map<String, String>> parse(File file, CSVFormat csvFormat, Charset charset) throws IOException
	{
		return parse(IOUtils.toBufferedInputStream(new FileInputStream(file)), csvFormat, charset);
	}

	/**
	 * Similar to {@link #parse(InputStream, CSVFormat, Charset, boolean)} with loading and parsing everything in memory first.
	 * 
	 * @param inputStream
	 * @param csvFormat
	 * @param charset
	 * @return
	 * @throws IOException
	 */
	public static Stream<Map<String, String>> parse(InputStream inputStream, CSVFormat csvFormat, Charset charset) throws IOException
	{
		boolean streaming = false;
		return parse(inputStream, csvFormat, charset, streaming);
	}

	/**
	 * Parses the given {@link InputStream} with the given {@link CSVFormat} and {@link Charset}. If streaming is set to true, the parser will parse record by
	 * record, but the returned {@link Stream} has to be closed explicitly, otherwise the whole content of the csv is parsed in memory first and than the result
	 * {@link Stream} returned without the need to close it manually.
	 * 
	 * @param inputStream
	 * @param csvFormat
	 * @param charset
	 * @param streaming
	 * @return
	 * @throws IOException
	 */
	public static Stream<Map<String, String>> parse(InputStream inputStream, CSVFormat csvFormat, Charset charset, boolean streaming) throws IOException
	{
		Stream<Map<String, String>> retval;

		Reader reader = new InputStreamReader(new BOMInputStream(inputStream), charset);
		CSVParser parser = new CSVParser(reader, csvFormat);

		retval = StreamUtils.fromIterator(parser.iterator())
							.map(record ->
							{
								Map<String, String> map = new LinkedHashMap<>();
								Map<String, Integer> headerMap = parser.getHeaderMap();
								if (headerMap != null)
								{
									for (String column : headerMap.keySet())
									{
										final String value = record.get(column);
										map.put(column, value);
									}
								}
								else
								{
									AtomicInteger index = new AtomicInteger();
									map.putAll(StreamUtils	.fromIterator(record.iterator())
															.collect(Collectors.toMap(value -> "" + index.getAndIncrement(), value -> value)));
								}
								return map;
							})
							.onClose(() ->
							{
								try
								{
									parser.close();
								} catch (IOException e)
								{
								}
								try
								{
									reader.close();
								} catch (IOException e)
								{
								}
							});

		if (!streaming)
		{
			List<Map<String, String>> list;
			try
			{
				list = retval.collect(Collectors.toList());
			} finally
			{
				retval.close();
			}
			retval = list.stream();
		}

		return retval;
	}

	public static interface Parser
	{
		ParserInputStreamLoaded from(InputStream inputStream);

		ParserInputStreamLoaded from(File file) throws FileNotFoundException;
	}

	public static interface ParserLoadedAndFormatDeclared
	{
		/**
		 * Activates the stream mode, which means the content of the given csv source is not loaded into memory, instead it is parsed as the {@link Stream} goes
		 * by. If this is not enabled, the parser will pull everything into memory first and parses everything before the parsed stream is returned.
		 * 
		 * @return this
		 */
		public ParserLoadedAndFormatDeclared enableStreaming();

		public Stream<Map<String, String>> get() throws IOException;
	}

	public static interface ParserLoaded extends ParserLoadedAndFormatDeclared
	{
		public ParserLoadedAndFormatDeclared withFormat(CSVFormat csvFormat);

		@Override
		public ParserLoaded enableStreaming();
	}

	public static interface ParserInputStreamLoaded extends ParserLoaded
	{
		/**
		 * Default is {@link StandardCharsets#UTF_8}
		 * 
		 * @see StandardCharsets
		 * @param charset
		 * @return
		 */
		public ParserLoaded withEncoding(Charset charset);
	}

	public static Parser parse()
	{
		return new Parser()
		{
			@Override
			public ParserInputStreamLoaded from(File file) throws FileNotFoundException
			{
				return this.from(new FileInputStream(file));
			}

			@Override
			public ParserInputStreamLoaded from(InputStream inputStream)
			{
				return new ParserInputStreamLoaded()
				{
					private Charset		charset		= StandardCharsets.UTF_8;
					private CSVFormat	csvFormat	= CSVFormat.EXCEL.withHeader();
					private boolean		streaming	= false;

					@Override
					public ParserLoaded withEncoding(Charset charset)
					{
						this.charset = charset;
						return this;
					}

					@Override
					public ParserLoadedAndFormatDeclared withFormat(CSVFormat csvFormat)
					{
						this.csvFormat = csvFormat;
						return this;
					}

					@Override
					public Stream<Map<String, String>> get() throws IOException
					{

						return CSVUtils.parse(inputStream, this.csvFormat, this.charset, this.streaming);
					}

					@Override
					public ParserLoaded enableStreaming()
					{
						this.streaming = true;
						return this;
					}
				};
			}
		};
	}

	public static interface SerializationOutput
	{
		/**
		 * Returns the csv content as {@link String}
		 *
		 * @return
		 */
		public String get();

		/**
		 * @see #withCharset(Charset)
		 * @param file
		 * @throws IOException
		 */
		public void writeTo(File file) throws IOException;

		/**
		 * Uses the given {@link Charset} in subsequent operations
		 *
		 * @param charset
		 * @return
		 */
		public SerializationOutput withCharset(Charset charset);
	}

	public static SerializationOutput serialize(Stream<Map<String, String>> data)
	{
		return serialize(data, CSVFormat.EXCEL.withHeader());
	}

	public static SerializationOutput serialize(Stream<Map<String, String>> data, CSVFormat csvFormat)
	{
		//
		Appendable serializationOutput = new StringBuilder();
		try
		{
			List<Map<String, String>> collectedData = data == null ? Collections.emptyList() : data	.filter(map -> map != null)
																									.collect(Collectors.toList());

			Set<String> headers = collectedData	.stream()
												.flatMap(map -> map	.keySet()
																	.stream())
												.collect(Collectors.toCollection(() -> new LinkedHashSet<>()));

			CSVPrinter csvPrinter = csvFormat	.withHeader(headers.toArray(new String[0]))
												.print(serializationOutput);
			collectedData	.stream()
							.forEach(map ->
							{
								try
								{
									csvPrinter.printRecord(map.values());
								} catch (IOException e)
								{
									throw new IllegalArgumentException(e);
								}
							});

			csvPrinter.close();
		} catch (Exception e)
		{
			throw new IllegalStateException(e);
		}

		//
		return new SerializationOutput()
		{
			private Charset charset = StandardCharsets.UTF_8;

			@Override
			public SerializationOutput withCharset(Charset charset)
			{
				this.charset = charset;
				return this;
			}

			@Override
			public void writeTo(File file) throws IOException
			{

				FileUtils.writeStringToFile(file, serializationOutput.toString(), this.charset);
			}

			@Override
			public String get()
			{
				return serializationOutput.toString();
			}
		};
	}
}
