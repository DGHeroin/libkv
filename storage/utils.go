package storage

import "strings"

func CreateEndpoints(endpoints []string, scheme string) (entries []string) {
    for _, addr := range endpoints {
        entries = append(entries, scheme+"://"+addr)
    }
    return entries
}

//     /path/to/key
func Normalize(key string) string {
    return "/" + join(SplitKey(key))
}

//     /path/to/
func GetDirectory(key string) string {
    parts := SplitKey(key)
    parts = parts[:len(parts)-1]
    return "/" + join(parts)
}

func SplitKey(key string) (path []string) {
    if strings.Contains(key, "/") {
        path = strings.Split(key, "/")
    } else {
        path = []string{key}
    }
    return path
}

func join(parts []string) string {
    return strings.Join(parts, "/")
}
