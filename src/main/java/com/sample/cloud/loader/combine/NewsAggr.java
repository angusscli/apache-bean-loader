package com.sample.cloud.loader.combine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.Objects;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn.Accumulator;

import com.google.common.base.MoreObjects;

public class NewsAggr {

	  private NewsAggr() { } // Namespace only

	  /**
	   * Returns a {@code PTransform} that takes an input
	   * {@code PCollection<NumT>} and returns a
	   * {@code PCollection<Double>} whose contents is the mean of the
	   * input {@code PCollection}'s elements, or
	   * {@code 0} if there are no elements.
	   *
	   * @param <NumT> the type of the {@code Number}s being combined
	   */
	  public static <NumT extends Number> Combine.Globally<NumT, String> globally() {
	    return Combine.globally(NewsAggr.of());
	  }

	  /**
	   * Returns a {@code PTransform} that takes an input
	   * {@code PCollection<KV<K, N>>} and returns a
	   * {@code PCollection<KV<K, Double>>} that contains an output
	   * element mapping each distinct key in the input
	   * {@code PCollection} to the mean of the values associated with
	   * that key in the input {@code PCollection}.
	   *
	   * <p>See {@link Combine.PerKey} for how this affects timestamps and bucketing.
	   *
	   * @param <K> the type of the keys
	   * @param <NumT> the type of the {@code Number}s being combined
	   */
	  public static <K, NumT extends Number> Combine.PerKey<K, NumT, String> perKey() {
	    return Combine.perKey(NewsAggr.of());
	  }

	  /**
	   * A {@code Combine.CombineFn} that computes the arithmetic mean
	   * (a.k.a. average) of an {@code Iterable} of numbers of type
	   * {@code N}, useful as an argument to {@link Combine#globally} or
	   * {@link Combine#perKey}.
	   *
	   * <p>Returns {@code Double.NaN} if combining zero elements.
	   *
	   * @param <NumT> the type of the {@code Number}s being combined
	   */
	  public static <NumT extends Number>
	  Combine.AccumulatingCombineFn<NumT, CountSum<NumT>, String> of() {
	    return new MeanFn<>();
	  }

	  /////////////////////////////////////////////////////////////////////////////

	  private static class MeanFn<NumT extends Number>
	  extends Combine.AccumulatingCombineFn<NumT, CountSum<NumT>, String> {
	    /**
	     * Constructs a combining function that computes the mean over
	     * a collection of values of type {@code N}.
	     */

	    @Override
	    public CountSum<NumT> createAccumulator() {
	      return new CountSum<>();
	    }

	    @Override
	    public Coder<CountSum<NumT>> getAccumulatorCoder(
	        CoderRegistry registry, Coder<NumT> inputCoder) {
	      return new CountSumCoder<>();
	    }
	  }

	  /**
	   * Accumulator class for {@link MeanFn}.
	   */
	  static class CountSum<NumT extends Number>
	  implements Accumulator<NumT, CountSum<NumT>, String> {

	    long count = 0;
	    double sum = 0.0;
	    long posCount = 0;
	    long negCount = 0;
	    long neutralCount = 0;

	    public CountSum() {
	      this(0, 0, 0, 0, 0);
	    }

	    public CountSum(long count, double sum, long posCount, long negCount, long neutralCount) {
	      this.count = count;
	      this.sum = sum;
	      this.posCount = posCount;
	      this.negCount = negCount;
	      this.neutralCount = neutralCount;
	    }

	    @Override
	    public void addInput(NumT element) {
	      count++;
	      sum += element.doubleValue();
	      if (element.doubleValue()>0) {
	    	  	posCount++;
	      } else if (element.doubleValue()==0)  {
	    	  	neutralCount++;
	      } else {
	    	  	negCount++;
	      }
	    }

	    @Override
	    public void mergeAccumulator(CountSum<NumT> accumulator) {
	      count += accumulator.count;
	      sum += accumulator.sum;
	      posCount += accumulator.posCount;
	      negCount += accumulator.negCount;
	      neutralCount += accumulator.neutralCount;
	    }

	    @Override
	    public String extractOutput() {
	    		double output = (count == 0 ? Double.NaN : sum / count);
	    		
	    		DecimalFormat df = new DecimalFormat("#.##");
	    		
	    		return "{\"avg\":"+Double.valueOf(df.format(output))+",\"pos\":"+posCount+",\"neg\":"+negCount+",\"neutral\":"+neutralCount+"}";
	    }

	    @Override
	    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
	        value = "FE_FLOATING_POINT_EQUALITY",
	        justification = "Comparing doubles directly since equals method is only used in coder test."
	    )
	    public boolean equals(Object other) {
	      if (!(other instanceof CountSum)) {
	        return false;
	      }
	      @SuppressWarnings("unchecked")
	      CountSum<?> otherCountSum = (CountSum<?>) other;
	      return (count == otherCountSum.count)
	          && (sum == otherCountSum.sum);
	    }

	    @Override
	    public int hashCode() {
	      return Objects.hash(count, sum);
	    }

	    @Override
	    public String toString() {
	      return MoreObjects.toStringHelper(this)
	          .add("count", count)
	          .add("sum", sum)
	          .toString();
	    }
	  }

	  static class CountSumCoder<NumT extends Number> extends AtomicCoder<CountSum<NumT>> {
	    private static final Coder<Long> LONG_CODER = BigEndianLongCoder.of();
	     private static final Coder<Double> DOUBLE_CODER = DoubleCoder.of();

	     @Override
	     public void encode(CountSum<NumT> value, OutputStream outStream)
	         throws CoderException, IOException {
	       LONG_CODER.encode(value.count, outStream);
	       DOUBLE_CODER.encode(value.sum, outStream);
	       LONG_CODER.encode(value.posCount, outStream);
	       LONG_CODER.encode(value.negCount, outStream);
	       LONG_CODER.encode(value.neutralCount, outStream);
	     }

	     @Override
	     public CountSum<NumT> decode(InputStream inStream)
	         throws CoderException, IOException {
	       return new CountSum<>(
	           LONG_CODER.decode(inStream),
	           DOUBLE_CODER.decode(inStream),
	           LONG_CODER.decode(inStream),
	           LONG_CODER.decode(inStream),
	           LONG_CODER.decode(inStream)
	           );
	    }

	    @Override
	    public void verifyDeterministic() throws NonDeterministicException {
	       LONG_CODER.verifyDeterministic();
	       DOUBLE_CODER.verifyDeterministic();
	    }
	  }
	}