import 'dart:convert';

import 'package:postgres/postgres.dart';

class PostgresTextEncoder {
  String convert(dynamic value, {bool escapeStrings = true}) {
    if (value == null) {
      return 'null';
    }

    if (value is int) {
      return _encodeNumber(value);
    }

    if (value is double) {
      return _encodeDouble(value);
    }

    if (value is String) {
      return _encodeString(value, escapeStrings);
    }

    if (value is DateTime) {
      return _encodeDateTime(value, isDateOnly: false);
    }

    if (value is Duration) {
      return _encodeDuration(value);
    }

    if (value is bool) {
      return _encodeBoolean(value);
    }

    if (value is Map) {
      return _encodeJSON(value, escapeStrings);
    }

    if (value is PgPoint) {
      return _encodePoint(value);
    }

    if (value is List) {
      return _encodeList(value);
    }

    // TODO: use custom type encoders

    throw PostgreSQLException("Could not infer type of value '$value'.");
  }

  String _encodeString(String text, bool escapeStrings) {
    if (!escapeStrings) {
      return text;
    }

    final backslashCodeUnit = r'\'.codeUnitAt(0);
    final quoteCodeUnit = r"'".codeUnitAt(0);

    var quoteCount = 0;
    var backslashCount = 0;
    final it = RuneIterator(text);
    while (it.moveNext()) {
      if (it.current == backslashCodeUnit) {
        backslashCount++;
      } else if (it.current == quoteCodeUnit) {
        quoteCount++;
      }
    }

    final buf = StringBuffer();

    if (backslashCount > 0) {
      buf.write(' E');
    }

    buf.write("'");

    if (quoteCount == 0 && backslashCount == 0) {
      buf.write(text);
    } else {
      text.codeUnits.forEach((i) {
        if (i == quoteCodeUnit || i == backslashCodeUnit) {
          buf.writeCharCode(i);
          buf.writeCharCode(i);
        } else {
          buf.writeCharCode(i);
        }
      });
    }

    buf.write("'");

    return buf.toString();
  }

  String _encodeNumber(num value) {
    if (value.isNaN) {
      return "'nan'";
    }

    if (value.isInfinite) {
      return value.isNegative ? "'-infinity'" : "'infinity'";
    }

    return value.toInt().toString();
  }

  String _encodeDouble(double value) {
    if (value.isNaN) {
      return "'nan'";
    }

    if (value.isInfinite) {
      return value.isNegative ? "'-infinity'" : "'infinity'";
    }

    return value.toString();
  }

  String _encodeBoolean(bool value) {
    return value ? 'TRUE' : 'FALSE';
  }

  String _encodeDateTime(DateTime value, {bool isDateOnly = false}) {
    var string = value.toIso8601String();

    if (isDateOnly) {
      string = string.split('T').first;
    } else {
      if (!value.isUtc) {
        final timezoneHourOffset = value.timeZoneOffset.inHours;
        final timezoneMinuteOffset = value.timeZoneOffset.inMinutes % 60;

        var hourComponent = timezoneHourOffset.abs().toString().padLeft(2, '0');
        final minuteComponent =
            timezoneMinuteOffset.abs().toString().padLeft(2, '0');

        if (timezoneHourOffset >= 0) {
          hourComponent = '+$hourComponent';
        } else {
          hourComponent = '-$hourComponent';
        }

        final timezoneString = [hourComponent, minuteComponent].join(':');
        string = [string, timezoneString].join('');
      }
    }

    if (string.substring(0, 1) == '-') {
      string = '${string.substring(1)} BC';
    } else if (string.substring(0, 1) == '+') {
      string = string.substring(1);
    }

    return "'$string'";
  }

  String _encodeDuration(Duration value) {
    return _DurationParts.fromDuration(value).forUsageInQuery();
  }

  String _encodeJSON(dynamic value, bool escapeStrings) {
    if (value == null) {
      return 'null';
    }

    if (value is String) {
      return "'${json.encode(value)}'";
    }

    return _encodeString(json.encode(value), escapeStrings);
  }

  String _encodePoint(PgPoint value) {
    return '(${_encodeDouble(value.latitude)}, ${_encodeDouble(value.longitude)})';
  }

  String _encodeList(List value) {
    if (value.isEmpty) {
      return '{}';
    }

    final type = value.fold(value.first.runtimeType, (type, item) {
      if (type == item.runtimeType) {
        return type;
      } else if ((type == int || type == double) && item is num) {
        return double;
      } else {
        return Map;
      }
    });

    if (type == bool) {
      return '{${value.cast<bool>().map((s) => s.toString()).join(',')}}';
    }

    if (type == int || type == double) {
      return '{${value.cast<num>().map((s) => s is double ? _encodeDouble(s) : _encodeNumber(s)).join(',')}}';
    }

    if (type == String) {
      return '{${value.cast<String>().map((s) {
        final escaped = s.replaceAll(r'\', r'\\').replaceAll('"', r'\"');
        return '"$escaped"';
      }).join(',')}}';
    }

    if (type == Map) {
      return '{${value.map((s) {
        final escaped =
            json.encode(s).replaceAll(r'\', r'\\').replaceAll('"', r'\"');

        return '"$escaped"';
      }).join(',')}}';
    }

    throw PostgreSQLException("Could not infer array type of value '$value'.");
  }
}

class _DurationParts {
  final int days;
  final int hours;
  final int minutes;
  final int seconds;
  final int milliseconds;
  final int microseconds;
  final bool isNegative;

  _DurationParts({
    required this.days,
    required this.hours,
    required this.minutes,
    required this.seconds,
    required this.milliseconds,
    required this.microseconds,
    required this.isNegative,
  });

  factory _DurationParts.fromDuration(Duration duration) {
    final isNegative = duration.isNegative;
    duration = duration.abs();

    var microseconds = duration.inMicroseconds;

    final days = microseconds ~/ Duration.microsecondsPerDay;
    microseconds %= Duration.microsecondsPerDay;
    final hours = microseconds ~/ Duration.microsecondsPerHour;
    microseconds %= Duration.microsecondsPerHour;
    final minutes = microseconds ~/ Duration.microsecondsPerMinute;
    microseconds %= Duration.microsecondsPerMinute;
    final seconds = microseconds ~/ Duration.microsecondsPerSecond;
    microseconds %= Duration.microsecondsPerSecond;
    final milliseconds = microseconds ~/ Duration.microsecondsPerMillisecond;
    microseconds %= Duration.microsecondsPerMillisecond;

    return _DurationParts(
      days: days,
      hours: hours,
      minutes: minutes,
      seconds: seconds,
      milliseconds: milliseconds,
      microseconds: microseconds,
      isNegative: isNegative,
    );
  }

  String forUsageInQuery() {
    Iterable<String> parts = [
      if (days > 0) '$days days',
      if (hours > 0) '$hours hours',
      if (minutes > 0) '$minutes minutes',
      if (seconds > 0) '$seconds seconds',
      if (milliseconds > 0) '$milliseconds milliseconds',
      if (microseconds > 0) '$microseconds microseconds',
    ];
    if (isNegative) parts = parts.map((p) => '-$p');
    return "'${parts.join(', ')}'";
  }
}
