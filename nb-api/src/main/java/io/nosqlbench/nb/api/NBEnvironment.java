package io.nosqlbench.nb.api;

import io.nosqlbench.nb.api.errors.BasicError;
import io.nosqlbench.nb.api.metadata.SessionNamer;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Safer, Easier lookup of property and environment variables, so
 * that commonly used env vars as seen on *n*x systems map to stable
 * system properties where known, but attempt to fall through to
 * the env variables if not.
 *
 * As long as all accesses and mutations for System properties and/or
 * environment variables are marshaled through a singleton instance
 * of this class, then users will not easily make a modify-after-reference
 * bug without a warning or error.
 *
 * Referencing a variable here means that a value was asked for under a
 * name, and a value was returned, even if this was the default value.
 *
 * Properties which contains a dot are presumed to be System properties,
 * and so in that case System properties take precedence for those.
 *
 * Environment variables which are known to map to a stable system property
 * are redirected to the system property.
 *
 * Otherwise, the environment variable is checked.
 *
 * Finally, System properties are checked for names not containing a dot.
 *
 * The first location to return a non-null value is used. Null values are considered
 * invalid in this API, except when provided as a default value.
 */
public class NBEnvironment {
    private Logger logger;

    // package private for testing
    NBEnvironment() {
    }

    public final static NBEnvironment INSTANCE = new NBEnvironment();

    private final LinkedHashMap<String, String> references = new LinkedHashMap<>();

    private final static Map<String, String> envToProp = Map.of(
        "PWD", "user.dir",
        "HOME", "user.home",
        "USERNAME", "user.name", // Win*
        "USER", "user.name", // *n*x
        "LOGNAME", "user.name" // *n*x
    );

    public NBEnvironment resetRefs() {
        this.references.clear();
        return this;
    }

    public void put(String propname, String value) {
        if (envToProp.containsKey(propname)) {
            throw new RuntimeException("The property you are changing should be considered immutable in this " +
                "process: '" + propname + "'");
        }
        if (references.containsKey(propname)) {
            if (references.get(propname).equals(value)) {
                if (logger != null) {
                    logger.warn("changing already referenced property '" + propname + "' to same value");
                }
            } else {
                throw new BasicError("Changing already referenced property '" + propname + "' from \n" +
                    "'" + references.get(propname) + "' to '" + value + "' is not supported.\n" +
                    " (maybe you can change the order of your options to set higher-level parameters first.)");
            }

        }
        System.setProperty(propname, value);
    }

    private String reference(String name, String value) {
        this.references.put(name, value);
        return value;
    }

    /**
     * Return the value in the first place it is found to be non-null,
     * or the default value otherwise.
     *
     * @param name         The system property or environment variable name
     * @param defaultValue The value to return if the name is not found
     * @return the system property or environment variable's value, or the default value
     */
    public String getOr(String name, String defaultValue) {
        String value = peek(name);
        if (value == null) {
            value = defaultValue;
        }
        return reference(name, value);
    }

    private String peek(String name) {
        String value = null;
        if (name.contains(".")) {
            value = System.getProperty(name.toLowerCase());
            if (value != null) {
                return value;
            }
        }
        if (envToProp.containsKey(name.toUpperCase())) {
            String propName = envToProp.get(name.toUpperCase());
            if (logger != null) {
                logger.debug("redirecting env var '" + name + "' to property '" + propName + "'");
            }
            value = System.getProperty(propName);
            if (value != null) {
                return value;
            }
        }
        value = System.getProperty(name);
        if (value != null) {
            return value;
        }

        value = System.getenv(name);
        return value;
    }

    /**
     * Attempt to read the variable in System properties or the shell environment. If it
     * is not found, throw an exception.
     *
     * @param name The System property or environment variable
     * @return the variable
     * @throws BasicError if no value was found which was non-null
     */
    public String get(String name) {
        String value = getOr(name, null);
        if (value == null) {
            throw new BasicError("No variable was found for '" + name + "' in system properties nor in the shell " +
                "environment.");
        }
        return value;
    }

    public boolean containsKey(String name) {
        String value = peek(name);
        return (value != null);
    }

    /**
     * For the given word, if it contains a pattern with '$' followed by alpha, followed
     * by alphanumeric and underscores, replace this pattern with the system property or
     * environment variable of the same name. Do this for all occurrences of this pattern.
     *
     * For patterns which start with '${' and end with '}', replace the contents with the same
     * rules as above, but allow any character in between.
     *
     * Nesting is not supported.
     *
     * @param word The word to interpolate the environment values into
     * @return The interpolated value, after substitutions, or null if any lookup failed
     */
    public Optional<String> interpolate(String word) {
        Pattern envpattern = Pattern.compile("(\\$(?<env1>[a-zA-Z_][A-Za-z0-9_.]+)|\\$\\{(?<env2>[^}]+)\\})");
        Matcher matcher = envpattern.matcher(word);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String envvar = matcher.group("env1");
            if (envvar == null) {
                envvar = matcher.group("env2");
            }
            String value = peek(envvar);
            if (value == null) {
                if (logger != null) {
                    logger.debug("no value found for '" + envvar + "', returning Optional.empty() for '" + word + "'");
                }
                return Optional.empty();
            } else {
                value = reference(envvar, value);
            }
            matcher.appendReplacement(sb, value);
        }
        matcher.appendTail(sb);
        return Optional.of(sb.toString());
    }

    public List<String> interpolateEach(CharSequence delim, String toBeRecombined) {
        String[] split = toBeRecombined.split(delim.toString());
        List<String> mapped = new ArrayList<>();
        for (String pattern : split) {
            Optional<String> interpolated = interpolate(pattern);
            interpolated.ifPresent(mapped::add);
        }
        return mapped;
    }

    /**
     * Interpolate system properties, environment variables, time fields, and arbitrary replacement strings
     * into a single result. Templates such as {@code /tmp/%d-${testrun}-$System.index-SCENARIO} are supported.
     *
     * <hr/>
     *
     * The tokens found in the raw template are interpolated in the following order.
     * <ul>
     *     <li>Any token which exactly matches one of the keys in the provided map is substituted
     *     directly as is. No token sigil like '$' is used here, so if you want to support that
     *     as is, you need to provide the keys in your substitution map as such.</li>
     *     <li>Any tokens in the form {@code %f} which is supported by the time fields in
     *     {@link Formatter}</li> are honored and used with the timestamp provided.*
     *     <li>System Properties: Any token in the form {@code $word.word} will be taken as the name
     *     of a system property to be substited.</li>
     *     <li>Environment Variables: Any token in the form {@code $name}</li> will be takens as
     *     an environment variable to be substituted.</li>
     * </ul>
     *
     * @param rawtext The template, including any of the supported token forms
     * @param millis  The timestamp to use for any temporal tokens
     * @param map     Any additional parameters to interpolate into the template first
     * @return Optionally, the interpolated string, as long as all references were qualified. Error
     * handling is contextual to the caller -- If not getting a valid result would cause a downstream error,
     * an error should likely be thrown.
     */
    public final Optional<String> interpolateWithTimestamp(String rawtext, long millis, Map<String, String> map) {
        String result = rawtext;
        for (String key : map.keySet()) {
            String value = map.get(key);
            result = result.replaceAll(Pattern.quote(key), value);
        }
        result = SessionNamer.format(result, millis);
        return interpolate(result);
    }

    public final Optional<String> interpolateWithTimestamp(String rawText, long millis) {
        return interpolateWithTimestamp(rawText, millis, Map.of());
    }

}
