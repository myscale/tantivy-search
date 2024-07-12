
pub struct TokenUtils;

impl TokenUtils {
    pub fn remove_nonsense_token(origin: Vec<&str>) -> Vec<&str> {
        let processed = origin
            .iter()
            .flat_map(|s| {
                s.split(|c: char | !c.is_alphanumeric())
                    .filter(|substr| !substr.is_empty())
                    .collect::<Vec<&str>>()
            })
            .collect();
        return processed;
    }
}