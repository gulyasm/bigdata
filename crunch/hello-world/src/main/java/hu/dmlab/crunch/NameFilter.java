package hu.dmlab.crunch;

import com.google.common.collect.ImmutableSet;
import org.apache.crunch.FilterFn;

import java.util.Set;

public class NameFilter extends FilterFn<String> {

    private static final Set<String> STOP_WORDS = ImmutableSet.copyOf(new String[]{
            "", "and", "the", "to", "of", "a", "in", "he", "his", "was", "that", "with",
            "had", "not", "at", "not", "as", "on", "her", "it", "but", "for", "what", "have",
            "one", "she", "is", "are", "they", "this", "by", "be", "i", "from", "or", "did", "when",
            "you", "him", "were", "which", "all", "up", "so", "been", "if", "my", "will", "into"
    });

    @Override
    public boolean accept(String arg0) {
        return !STOP_WORDS.contains(arg0);
    }
}
            gi